/*!
# cuda-discovery

Service discovery for agent fleets.

How does an agent find another agent? How does the fleet know who's
alive, what they can do, and where they are? This crate provides a
service directory with registration, health probing, and capability
matching.

- Agent registration with metadata
- Health status tracking
- Capability-based lookup
- Endpoint management
- TTL-based expiration
- Fleet directory queries
*/

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Agent service entry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceEntry {
    pub agent_id: String,
    pub name: String,
    pub endpoint: String,       // how to reach this agent
    pub capabilities: Vec<String>,
    pub version: String,
    pub metadata: HashMap<String, String>,
    pub registered_ms: u64,
    pub last_heartbeat_ms: u64,
    pub ttl_ms: u64,
    pub healthy: bool,
    pub load: f64,             // 0-1 current load
}

impl ServiceEntry {
    pub fn has_capability(&self, cap: &str) -> bool {
        self.capabilities.iter().any(|c| c == cap)
    }

    pub fn is_expired(&self) -> bool {
        now() - self.last_heartbeat_ms > self.ttl_ms
    }
}

/// Discovery query
#[derive(Clone, Debug)]
pub struct DiscoveryQuery {
    pub capability: Option<String>,
    pub healthy_only: bool,
    pub min_version: Option<String>,
    pub max_load: Option<f64>,
    pub limit: usize,
}

impl Default for DiscoveryQuery {
    fn default() -> Self { DiscoveryQuery { capability: None, healthy_only: true, min_version: None, max_load: None, limit: 10 } }
}

impl DiscoveryQuery {
    pub fn by_capability(cap: &str) -> Self {
        let mut q = DiscoveryQuery::default();
        q.capability = Some(cap.to_string());
        q
    }

    pub fn include_unhealthy(mut self) -> Self { self.healthy_only = false; self }
    pub fn with_limit(mut self, n: usize) -> Self { self.limit = n; self }
}

/// Service discovery registry
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoveryRegistry {
    pub services: HashMap<String, ServiceEntry>,
    pub default_ttl_ms: u64,
    pub total_registrations: u64,
    pub total_deregistrations: u64,
    pub total_lookups: u64,
}

impl DiscoveryRegistry {
    pub fn new() -> Self { DiscoveryRegistry { services: HashMap::new(), default_ttl_ms: 60_000, total_registrations: 0, total_deregistrations: 0, total_lookups: 0 } }

    /// Register an agent
    pub fn register(&mut self, entry: ServiceEntry) {
        let id = entry.agent_id.clone();
        self.total_registrations += 1;
        self.services.insert(id, entry);
    }

    /// Quick register helper
    pub fn register_simple(&mut self, agent_id: &str, name: &str, endpoint: &str, capabilities: &[&str]) {
        let entry = ServiceEntry {
            agent_id: agent_id.to_string(), name: name.to_string(), endpoint: endpoint.to_string(),
            capabilities: capabilities.iter().map(|c| c.to_string()).collect(),
            version: "0.1.0".into(), metadata: HashMap::new(),
            registered_ms: now(), last_heartbeat_ms: now(), ttl_ms: self.default_ttl_ms,
            healthy: true, load: 0.0,
        };
        self.register(entry);
    }

    /// Deregister an agent
    pub fn deregister(&mut self, agent_id: &str) -> bool {
        if self.services.remove(agent_id).is_some() { self.total_deregistrations += 1; true }
        else { false }
    }

    /// Heartbeat (update last seen)
    pub fn heartbeat(&mut self, agent_id: &str, load: f64) -> bool {
        if let Some(entry) = self.services.get_mut(agent_id) {
            entry.last_heartbeat_ms = now();
            entry.load = load;
            entry.healthy = true;
            true
        } else { false }
    }

    /// Discover services matching query
    pub fn discover(&mut self, query: &DiscoveryQuery) -> Vec<&ServiceEntry> {
        self.total_lookups += 1;
        self.gc();
        let mut results: Vec<&ServiceEntry> = self.services.values().filter(|s| {
            if query.healthy_only && !s.healthy { return false; }
            if let Some(ref cap) = query.capability { if !s.has_capability(cap) { return false; } }
            if let Some(ref ver) = query.min_version { if !version_gte(&s.version, ver) { return false; } }
            if let Some(max_load) = query.max_load { if s.load > max_load { return false; } }
            true
        }).collect();
        // Sort by load (lowest first) then by registration time (longest-running first)
        results.sort_by(|a, b| a.load.partial_cmp(&b.load).unwrap_or(std::cmp::Ordering::Equal).then(b.registered_ms.cmp(&a.registered_ms)));
        results.truncate(query.limit);
        results
    }

    /// Find least-loaded agent with a capability
    pub fn find_least_loaded(&mut self, capability: &str) -> Option<&ServiceEntry> {
        let query = DiscoveryQuery::by_capability(capability).with_limit(1);
        self.discover(&query).into_iter().next()
    }

    /// Get a specific service
    pub fn get(&self, agent_id: &str) -> Option<&ServiceEntry> { self.services.get(agent_id) }

    /// Mark agent as unhealthy
    pub fn mark_unhealthy(&mut self, agent_id: &str) {
        if let Some(entry) = self.services.get_mut(agent_id) { entry.healthy = false; }
    }

    /// Garbage collect expired entries
    pub fn gc(&mut self) -> usize {
        let expired: Vec<String> = self.services.iter().filter(|(_, s)| s.is_expired()).map(|(id, _)| id.clone()).collect();
        for id in expired { self.deregister(&id); }
        expired.len()
    }

    /// All capabilities in the fleet
    pub fn all_capabilities(&self) -> Vec<String> {
        let mut caps: Vec<String> = self.services.values().flat_map(|s| s.capabilities.clone()).collect();
        caps.sort();
        caps.dedup();
        caps
    }

    /// Agent count
    pub fn len(&self) -> usize { self.services.len() }

    /// Summary
    pub fn summary(&self) -> String {
        let healthy = self.services.values().filter(|s| s.healthy).count();
        let avg_load = if self.services.is_empty() { 0.0 } else { self.services.values().map(|s| s.load).sum::<f64>() / self.services.len() as f64 };
        format!("Discovery: {} services ({}/{} healthy), {} lookups, avg_load={:.0}%",
            self.services.len(), healthy, self.services.len(), self.total_lookups, avg_load * 100.0)
    }
}

fn now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

/// Simple semver-ish version comparison (a.b.c >= min)
fn version_gte(version: &str, min: &str) -> bool {
    let parse = |v: &str| -> Vec<u32> { v.split('.').filter_map(|p| p.parse().ok()).collect() };
    let v = parse(version);
    let m = parse(min);
    for i in 0..3 {
        let vi = v.get(i).copied().unwrap_or(0);
        let mi = m.get(i).copied().unwrap_or(0);
        if vi > mi { return true; }
        if vi < mi { return false; }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_find() {
        let mut reg = DiscoveryRegistry::new();
        reg.register_simple("a1", "Navigator", "nav://a1", &["pathfinding", "routing"]);
        reg.register_simple("a2", "Sensor", "sense://a2", &["perception", "vision"]);
        let result = reg.discover(&DiscoveryQuery::by_capability("pathfinding"));
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].agent_id, "a1");
    }

    #[test]
    fn test_least_loaded() {
        let mut reg = DiscoveryRegistry::new();
        reg.register_simple("a1", "A", "://a1", &["work"]);
        reg.register_simple("a2", "A", "://a2", &["work"]);
        reg.heartbeat("a1", 0.8);
        reg.heartbeat("a2", 0.2);
        let found = reg.find_least_loaded("work").unwrap();
        assert_eq!(found.agent_id, "a2");
    }

    #[test]
    fn test_heartbeat() {
        let mut reg = DiscoveryRegistry::new();
        reg.register_simple("a1", "A", "://a1", &[]);
        reg.heartbeat("a1", 0.5);
        assert_eq!(reg.get("a1").unwrap().load, 0.5);
    }

    #[test]
    fn test_deregister() {
        let mut reg = DiscoveryRegistry::new();
        reg.register_simple("a1", "A", "://a1", &[]);
        assert!(reg.deregister("a1"));
        assert!(reg.get("a1").is_none());
    }

    #[test]
    fn test_unhealthy_filtered() {
        let mut reg = DiscoveryRegistry::new();
        reg.register_simple("a1", "A", "://a1", &["work"]);
        reg.register_simple("a2", "B", "://a2", &["work"]);
        reg.mark_unhealthy("a2");
        let results = reg.discover(&DiscoveryQuery::by_capability("work"));
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_version_filter() {
        let mut reg = DiscoveryRegistry::new();
        let mut entry = ServiceEntry { agent_id: "a1".into(), name: "A".into(), endpoint: "://a1".into(), capabilities: vec!["x".into()], version: "2.0.0".into(), metadata: HashMap::new(), registered_ms: now(), last_heartbeat_ms: now(), ttl_ms: 60_000, healthy: true, load: 0.0 };
        reg.register(entry);
        let q = DiscoveryQuery::by_capability("x");
        let q = DiscoveryQuery { min_version: Some("1.5.0".into()), ..q };
        assert_eq!(reg.discover(&q).len(), 1);
        let q2 = DiscoveryQuery { min_version: Some("3.0.0".into()), ..DiscoveryQuery::by_capability("x") };
        assert_eq!(reg.discover(&q2).len(), 0);
    }

    #[test]
    fn test_gc_expired() {
        let mut reg = DiscoveryRegistry::new();
        let mut entry = ServiceEntry { agent_id: "old".into(), name: "Old".into(), endpoint: "://old".into(), capabilities: vec![], version: "1.0.0".into(), metadata: HashMap::new(), registered_ms: 0, last_heartbeat_ms: 0, ttl_ms: 1, healthy: true, load: 0.0 };
        reg.register(entry);
        let removed = reg.gc();
        assert_eq!(removed, 1);
    }

    #[test]
    fn test_all_capabilities() {
        let mut reg = DiscoveryRegistry::new();
        reg.register_simple("a1", "A", "://a1", &["nav", "plan"]);
        reg.register_simple("a2", "B", "://a2", &["nav", "sense"]);
        let caps = reg.all_capabilities();
        assert!(caps.contains(&"nav".to_string()));
        assert!(caps.contains(&"plan".to_string()));
        assert!(caps.contains(&"sense".to_string()));
    }

    #[test]
    fn test_max_load_filter() {
        let mut reg = DiscoveryRegistry::new();
        reg.register_simple("a1", "A", "://a1", &["work"]);
        reg.heartbeat("a1", 0.9);
        let q = DiscoveryQuery { capability: Some("work".into()), max_load: Some(0.5), ..DiscoveryQuery::default() };
        assert_eq!(reg.discover(&q).len(), 0);
    }

    #[test]
    fn test_summary() {
        let reg = DiscoveryRegistry::new();
        let s = reg.summary();
        assert!(s.contains("0 services"));
    }
}
