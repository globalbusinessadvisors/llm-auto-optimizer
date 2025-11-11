use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A composite aggregator that runs multiple aggregations in parallel
///
/// This is useful when you want to compute multiple statistics at once
/// (e.g., count, avg, p95, stddev) without making multiple passes over the data.
///
/// # Examples
///
/// ```
/// use processor::aggregation::{CompositeAggregator, AggregationResult};
///
/// let mut composite = CompositeAggregator::new();
/// composite.add_count("request_count");
/// composite.add_avg("avg_latency");
/// composite.add_p95("p95_latency");
/// composite.add_stddev("stddev_latency");
///
/// // Feed values
/// for latency in vec![100.0, 150.0, 200.0, 250.0, 300.0] {
///     composite.update(latency).unwrap();
/// }
///
/// // Get all results
/// let results = composite.finalize().unwrap();
/// println!("Count: {}", results.get_count("request_count").unwrap());
/// println!("Avg: {}", results.get_value("avg_latency").unwrap());
/// println!("P95: {}", results.get_value("p95_latency").unwrap());
/// println!("StdDev: {}", results.get_value("stddev_latency").unwrap());
/// ```
#[derive(Debug)]
pub struct CompositeAggregator {
    aggregations: Vec<AggregationSpec>,
}

#[derive(Debug, Clone)]
enum AggregationSpec {
    Count {
        name: String,
        count: u64,
    },
    Sum {
        name: String,
        sum: f64,
        count: u64,
    },
    Avg {
        name: String,
        sum: f64,
        count: u64,
    },
    Min {
        name: String,
        min: Option<f64>,
        count: u64,
    },
    Max {
        name: String,
        max: Option<f64>,
        count: u64,
    },
    Percentile {
        name: String,
        values: Vec<f64>,
        percentile: f64,
    },
    StdDev {
        name: String,
        count: u64,
        mean: f64,
        m2: f64,
    },
}

/// Results from a composite aggregation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationResult {
    counts: HashMap<String, u64>,
    values: HashMap<String, f64>,
}

impl AggregationResult {
    pub fn new() -> Self {
        Self {
            counts: HashMap::new(),
            values: HashMap::new(),
        }
    }

    /// Set a count result
    pub fn set_count(&mut self, name: impl Into<String>, count: u64) {
        self.counts.insert(name.into(), count);
    }

    /// Set a numeric result
    pub fn set_value(&mut self, name: impl Into<String>, value: f64) {
        self.values.insert(name.into(), value);
    }

    /// Get a count result by name
    pub fn get_count(&self, name: &str) -> Option<u64> {
        self.counts.get(name).copied()
    }

    /// Get a numeric result by name
    pub fn get_value(&self, name: &str) -> Option<f64> {
        self.values.get(name).copied()
    }

    /// Get all count results
    pub fn counts(&self) -> &HashMap<String, u64> {
        &self.counts
    }

    /// Get all numeric results
    pub fn values(&self) -> &HashMap<String, f64> {
        &self.values
    }
}

impl CompositeAggregator {
    /// Create a new composite aggregator
    pub fn new() -> Self {
        Self {
            aggregations: Vec::new(),
        }
    }

    /// Add a count aggregation
    pub fn add_count(&mut self, name: impl Into<String>) -> &mut Self {
        self.aggregations.push(AggregationSpec::Count {
            name: name.into(),
            count: 0,
        });
        self
    }

    /// Add a sum aggregation
    pub fn add_sum(&mut self, name: impl Into<String>) -> &mut Self {
        self.aggregations.push(AggregationSpec::Sum {
            name: name.into(),
            sum: 0.0,
            count: 0,
        });
        self
    }

    /// Add an average aggregation
    pub fn add_avg(&mut self, name: impl Into<String>) -> &mut Self {
        self.aggregations.push(AggregationSpec::Avg {
            name: name.into(),
            sum: 0.0,
            count: 0,
        });
        self
    }

    /// Add a min aggregation
    pub fn add_min(&mut self, name: impl Into<String>) -> &mut Self {
        self.aggregations.push(AggregationSpec::Min {
            name: name.into(),
            min: None,
            count: 0,
        });
        self
    }

    /// Add a max aggregation
    pub fn add_max(&mut self, name: impl Into<String>) -> &mut Self {
        self.aggregations.push(AggregationSpec::Max {
            name: name.into(),
            max: None,
            count: 0,
        });
        self
    }

    /// Add a percentile aggregation
    pub fn add_percentile(&mut self, name: impl Into<String>, percentile: f64) -> anyhow::Result<&mut Self> {
        if !(0.0..=100.0).contains(&percentile) {
            return Err(anyhow!("Percentile must be between 0 and 100"));
        }
        self.aggregations.push(AggregationSpec::Percentile {
            name: name.into(),
            values: Vec::new(),
            percentile,
        });
        Ok(self)
    }

    /// Add a p50 (median) aggregation
    pub fn add_p50(&mut self, name: impl Into<String>) -> &mut Self {
        self.aggregations.push(AggregationSpec::Percentile {
            name: name.into(),
            values: Vec::new(),
            percentile: 50.0,
        });
        self
    }

    /// Add a p95 aggregation
    pub fn add_p95(&mut self, name: impl Into<String>) -> &mut Self {
        self.aggregations.push(AggregationSpec::Percentile {
            name: name.into(),
            values: Vec::new(),
            percentile: 95.0,
        });
        self
    }

    /// Add a p99 aggregation
    pub fn add_p99(&mut self, name: impl Into<String>) -> &mut Self {
        self.aggregations.push(AggregationSpec::Percentile {
            name: name.into(),
            values: Vec::new(),
            percentile: 99.0,
        });
        self
    }

    /// Add a standard deviation aggregation
    pub fn add_stddev(&mut self, name: impl Into<String>) -> &mut Self {
        self.aggregations.push(AggregationSpec::StdDev {
            name: name.into(),
            count: 0,
            mean: 0.0,
            m2: 0.0,
        });
        self
    }

    /// Update all aggregations with a new value
    pub fn update(&mut self, value: f64) -> anyhow::Result<()> {
        for agg in &mut self.aggregations {
            match agg {
                AggregationSpec::Count { count, .. } => {
                    *count += 1;
                }
                AggregationSpec::Sum { sum, count, .. } => {
                    *sum += value;
                    *count += 1;
                }
                AggregationSpec::Avg { sum, count, .. } => {
                    *sum += value;
                    *count += 1;
                }
                AggregationSpec::Min { min, count, .. } => {
                    *min = Some(match *min {
                        Some(current) => current.min(value),
                        None => value,
                    });
                    *count += 1;
                }
                AggregationSpec::Max { max, count, .. } => {
                    *max = Some(match *max {
                        Some(current) => current.max(value),
                        None => value,
                    });
                    *count += 1;
                }
                AggregationSpec::Percentile { values, .. } => {
                    values.push(value);
                }
                AggregationSpec::StdDev { count, mean, m2, .. } => {
                    *count += 1;
                    let delta = value - *mean;
                    *mean += delta / *count as f64;
                    let delta2 = value - *mean;
                    *m2 += delta * delta2;
                }
            }
        }
        Ok(())
    }

    /// Update all aggregations with multiple values
    pub fn update_batch(&mut self, values: &[f64]) -> anyhow::Result<()> {
        for value in values {
            self.update(*value)?;
        }
        Ok(())
    }

    /// Compute final results for all aggregations
    pub fn finalize(&self) -> anyhow::Result<AggregationResult> {
        let mut result = AggregationResult::new();

        for agg in &self.aggregations {
            match agg {
                AggregationSpec::Count { name, count } => {
                    result.counts.insert(name.clone(), *count);
                }
                AggregationSpec::Sum { name, sum, .. } => {
                    result.values.insert(name.clone(), *sum);
                }
                AggregationSpec::Avg { name, sum, count } => {
                    if *count == 0 {
                        return Err(anyhow!("Cannot compute average '{}' of zero values", name));
                    }
                    result.values.insert(name.clone(), *sum / *count as f64);
                }
                AggregationSpec::Min { name, min, .. } => {
                    let value = min.ok_or_else(|| anyhow!("Cannot compute min '{}' of zero values", name))?;
                    result.values.insert(name.clone(), value);
                }
                AggregationSpec::Max { name, max, .. } => {
                    let value = max.ok_or_else(|| anyhow!("Cannot compute max '{}' of zero values", name))?;
                    result.values.insert(name.clone(), value);
                }
                AggregationSpec::Percentile { name, values, percentile } => {
                    if values.is_empty() {
                        return Err(anyhow!("Cannot compute percentile '{}' of zero values", name));
                    }
                    let mut sorted = values.clone();
                    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    let value = calculate_percentile(&sorted, *percentile)
                        .ok_or_else(|| anyhow!("Failed to calculate percentile '{}'", name))?;
                    result.values.insert(name.clone(), value);
                }
                AggregationSpec::StdDev { name, count, m2, .. } => {
                    if *count < 2 {
                        return Err(anyhow!("Cannot compute stddev '{}' with less than 2 values", name));
                    }
                    let variance = *m2 / (*count - 1) as f64;
                    result.values.insert(name.clone(), variance.sqrt());
                }
            }
        }

        Ok(result)
    }

    /// Reset all aggregations to initial state
    pub fn reset(&mut self) {
        for agg in &mut self.aggregations {
            match agg {
                AggregationSpec::Count { count, .. } => {
                    *count = 0;
                }
                AggregationSpec::Sum { sum, count, .. } => {
                    *sum = 0.0;
                    *count = 0;
                }
                AggregationSpec::Avg { sum, count, .. } => {
                    *sum = 0.0;
                    *count = 0;
                }
                AggregationSpec::Min { min, count, .. } => {
                    *min = None;
                    *count = 0;
                }
                AggregationSpec::Max { max, count, .. } => {
                    *max = None;
                    *count = 0;
                }
                AggregationSpec::Percentile { values, .. } => {
                    values.clear();
                }
                AggregationSpec::StdDev { count, mean, m2, .. } => {
                    *count = 0;
                    *mean = 0.0;
                    *m2 = 0.0;
                }
            }
        }
    }

    /// Get the number of aggregations
    pub fn len(&self) -> usize {
        self.aggregations.len()
    }

    /// Check if there are no aggregations
    pub fn is_empty(&self) -> bool {
        self.aggregations.is_empty()
    }
}

impl Default for CompositeAggregator {
    fn default() -> Self {
        Self::new()
    }
}

fn calculate_percentile(sorted_values: &[f64], percentile: f64) -> Option<f64> {
    if sorted_values.is_empty() {
        return None;
    }

    if sorted_values.len() == 1 {
        return Some(sorted_values[0]);
    }

    let rank = (percentile / 100.0) * (sorted_values.len() - 1) as f64;
    let lower_idx = rank.floor() as usize;
    let upper_idx = rank.ceil() as usize;

    if lower_idx == upper_idx {
        Some(sorted_values[lower_idx])
    } else {
        let fraction = rank - lower_idx as f64;
        Some(sorted_values[lower_idx] * (1.0 - fraction)
            + sorted_values[upper_idx] * fraction)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_composite_basic() {
        let mut composite = CompositeAggregator::new();
        composite
            .add_count("count")
            .add_sum("sum")
            .add_avg("avg")
            .add_min("min")
            .add_max("max");

        composite.update(10.0).unwrap();
        composite.update(20.0).unwrap();
        composite.update(30.0).unwrap();

        let results = composite.finalize().unwrap();

        assert_eq!(results.get_count("count").unwrap(), 3);
        assert_eq!(results.get_value("sum").unwrap(), 60.0);
        assert_eq!(results.get_value("avg").unwrap(), 20.0);
        assert_eq!(results.get_value("min").unwrap(), 10.0);
        assert_eq!(results.get_value("max").unwrap(), 30.0);
    }

    #[test]
    fn test_composite_percentiles() {
        let mut composite = CompositeAggregator::new();
        composite
            .add_p50("p50")
            .add_p95("p95")
            .add_p99("p99");

        for i in 1..=100 {
            composite.update(i as f64).unwrap();
        }

        let results = composite.finalize().unwrap();

        assert!((results.get_value("p50").unwrap() - 50.5).abs() < 0.1);
        assert!((results.get_value("p95").unwrap() - 95.05).abs() < 0.1);
        assert!((results.get_value("p99").unwrap() - 99.01).abs() < 0.1);
    }

    #[test]
    fn test_composite_stddev() {
        let mut composite = CompositeAggregator::new();
        composite
            .add_avg("avg")
            .add_stddev("stddev");

        composite.update(10.0).unwrap();
        composite.update(12.0).unwrap();
        composite.update(14.0).unwrap();
        composite.update(16.0).unwrap();
        composite.update(18.0).unwrap();

        let results = composite.finalize().unwrap();

        assert!((results.get_value("avg").unwrap() - 14.0).abs() < 0.01);
        assert!((results.get_value("stddev").unwrap() - 3.162).abs() < 0.01);
    }

    #[test]
    fn test_composite_batch() {
        let mut composite = CompositeAggregator::new();
        composite
            .add_count("count")
            .add_avg("avg");

        composite.update_batch(&[10.0, 20.0, 30.0, 40.0, 50.0]).unwrap();

        let results = composite.finalize().unwrap();

        assert_eq!(results.get_count("count").unwrap(), 5);
        assert_eq!(results.get_value("avg").unwrap(), 30.0);
    }

    #[test]
    fn test_composite_reset() {
        let mut composite = CompositeAggregator::new();
        composite.add_count("count").add_sum("sum");

        composite.update(10.0).unwrap();
        composite.update(20.0).unwrap();

        composite.reset();

        composite.update(5.0).unwrap();

        let results = composite.finalize().unwrap();

        assert_eq!(results.get_count("count").unwrap(), 1);
        assert_eq!(results.get_value("sum").unwrap(), 5.0);
    }

    #[test]
    fn test_composite_empty() {
        let composite = CompositeAggregator::new();
        assert!(composite.is_empty());
        assert_eq!(composite.len(), 0);
    }

    #[test]
    fn test_composite_all_aggregations() {
        let mut composite = CompositeAggregator::new();
        composite
            .add_count("count")
            .add_sum("sum")
            .add_avg("avg")
            .add_min("min")
            .add_max("max")
            .add_p50("p50")
            .add_p95("p95")
            .add_stddev("stddev");

        for i in 1..=100 {
            composite.update(i as f64).unwrap();
        }

        let results = composite.finalize().unwrap();

        assert_eq!(results.get_count("count").unwrap(), 100);
        assert_eq!(results.get_value("sum").unwrap(), 5050.0);
        assert_eq!(results.get_value("avg").unwrap(), 50.5);
        assert_eq!(results.get_value("min").unwrap(), 1.0);
        assert_eq!(results.get_value("max").unwrap(), 100.0);
        assert!((results.get_value("p50").unwrap() - 50.5).abs() < 0.1);
        assert!((results.get_value("p95").unwrap() - 95.05).abs() < 0.1);
        assert!((results.get_value("stddev").unwrap() - 29.01).abs() < 0.5);
    }

    #[test]
    fn test_composite_serialization() {
        let mut composite = CompositeAggregator::new();
        composite.add_count("count").add_avg("avg");

        composite.update(10.0).unwrap();
        composite.update(20.0).unwrap();

        let results = composite.finalize().unwrap();
        let serialized = serde_json::to_string(&results).unwrap();
        let deserialized: AggregationResult = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.get_count("count").unwrap(), 2);
        assert_eq!(deserialized.get_value("avg").unwrap(), 15.0);
    }

    #[test]
    fn test_composite_custom_percentile() {
        let mut composite = CompositeAggregator::new();
        composite.add_percentile("p75", 75.0).unwrap();

        for i in 1..=100 {
            composite.update(i as f64).unwrap();
        }

        let results = composite.finalize().unwrap();
        assert!((results.get_value("p75").unwrap() - 75.25).abs() < 0.5);
    }

    #[test]
    fn test_composite_invalid_percentile() {
        let mut composite = CompositeAggregator::new();
        assert!(composite.add_percentile("invalid", 150.0).is_err());
    }

    #[test]
    fn test_real_world_latency_tracking() {
        // Simulate tracking request latencies
        let mut composite = CompositeAggregator::new();
        composite
            .add_count("request_count")
            .add_avg("avg_latency_ms")
            .add_min("min_latency_ms")
            .add_max("max_latency_ms")
            .add_p50("p50_latency_ms")
            .add_p95("p95_latency_ms")
            .add_p99("p99_latency_ms")
            .add_stddev("stddev_latency_ms");

        // Simulate some request latencies
        let latencies = vec![
            45.0, 52.0, 48.0, 51.0, 49.0, // Normal requests
            150.0, 200.0, // Slower requests
            46.0, 50.0, 47.0, // More normal
            500.0, // One outlier
        ];

        for latency in latencies {
            composite.update(latency).unwrap();
        }

        let results = composite.finalize().unwrap();

        println!("Latency Statistics:");
        println!("  Total Requests: {}", results.get_count("request_count").unwrap());
        println!("  Avg: {:.2}ms", results.get_value("avg_latency_ms").unwrap());
        println!("  Min: {:.2}ms", results.get_value("min_latency_ms").unwrap());
        println!("  Max: {:.2}ms", results.get_value("max_latency_ms").unwrap());
        println!("  P50: {:.2}ms", results.get_value("p50_latency_ms").unwrap());
        println!("  P95: {:.2}ms", results.get_value("p95_latency_ms").unwrap());
        println!("  P99: {:.2}ms", results.get_value("p99_latency_ms").unwrap());
        println!("  StdDev: {:.2}ms", results.get_value("stddev_latency_ms").unwrap());

        assert_eq!(results.get_count("request_count").unwrap(), 11);
        assert!(results.get_value("avg_latency_ms").unwrap() > 50.0);
        assert_eq!(results.get_value("min_latency_ms").unwrap(), 45.0);
        assert_eq!(results.get_value("max_latency_ms").unwrap(), 500.0);
    }
}
