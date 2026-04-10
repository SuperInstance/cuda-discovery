/*!
# cuda-discovery

Agent capability discovery and peer finding.

Agents need to find each other. What can you do? Who can help with
this task? Discovery answers these questions through capability
broadcasting, peer matching, and fleet topology awareness.

- Agent descriptor (capabilities, location, status)
- Capability registry with fuzzy matching
- Peer discovery by capability requirements
- Fleet topology (connected components)
- Presence (online/offline/busy)
- Recommendation engine (who should I talk to?)
*/

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Agent presence
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Presence { Online, Offline, Busy, Away }

/// An agent descriptor
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AgentDescriptor {
    pub id: String,
    pub name: String,
    pub capabilities: Vec<String>,
    pub location: Option<String>,
    pub presence: Presence,
    pub version: String,
    pub metadata: HashMap<String, String>,
    pub last_seen_ms: u64,
    pub trust_score: f64,
}

impl AgentDescriptor {
    pub fn new(id: &str, name: &str) -> Self {
        AgentDescriptor { id: id.to_string(), name: name.to_string(), capabilities: vec![], location: None, presence: Presence::Online, version: "0.1.0".into(), metadata: HashMap::new(), last_seen_ms: now(), trust_score: 0.5 }
    }

    pub fn with_capability(mut self, cap: &str) -> Self { self.capabilities.push(cap.to_string()); self }

    fn match_score(&self, required: &[String]) -> f64 {
        if required.is_empty() { return 1.0; }
        let matches = required.iter().filter(|r| self.capabilities.contains(r)).count();
        matches as f64 / required.len() as f64
    }
}

/// Discovery query
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoveryQuery {
    pub required_capabilities: Vec<String>,
    pub preferred_capabilities: Vec<String>,
    pub max_results: usize,
    pub exclude_agents: HashSet<String>,
    pub min_trust: f64,
}

impl Default for DiscoveryQuery {
    fn default() -> Self { DiscoveryQuery { required_capabilities: vec![], preferred_capabilities: vec![], max_results: 10, exclude_agents: HashSet::new(), min_trust: 0.0 } }
}

/// A discovery result
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoveryResult {
    pub agent: AgentDescriptor,
    pub match_score: f64,
    pub trust_bonus: f64,
    pub composite_score: f64,
}

/// The discovery system
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DiscoverySystem {
    pub agents: HashMap<String, AgentDescriptor>,
    pub capability_index: HashMap<String, Vec<String>>, // cap → [agent_ids]
    pub total_queries: u64,
    pub total_matches: u64,
}

impl DiscoverySystem {
    pub fn new() -> Self { DiscoverySystem { agents: HashMap::new(), capability_index: HashMap::new(), total_queries: 0, total_matches: 0 } }

    /// Register an agent
    pub fn register(&mut self, descriptor: AgentDescriptor) {
        let id = descriptor.id.clone();
        // Remove old capability index entries
        if let Some(old) = self.agents.get(&id) {
            for cap in &old.capabilities {
                if let Some(ids) = self.capability_index.get_mut(cap) {
                    ids.retain(|i| i != &id);
                }
            }
        }
        // Add new
        for cap in &descriptor.capabilities {
            self.capability_index.entry(cap.clone()).or_insert_with(Vec::new).push(id.clone());
        }
        self.agents.insert(id, descriptor);
    }

    /// Deregister
    pub fn deregister(&mut self, agent_id: &str) {
        if let Some(agent) = self.agents.remove(agent_id) {
            for cap in agent.capabilities {
                if let Some(ids) = self.capability_index.get_mut(&cap) {
                    ids.retain(|i| i != agent_id);
                }
            }
        }
    }

    /// Update presence
    pub fn set_presence(&mut self, agent_id: &str, presence: Presence) {
        if let Some(agent) = self.agents.get_mut(agent_id) {
            agent.presence = presence;
            agent.last_seen_ms = now();
        }
    }

    /// Discover agents matching a query
    pub fn discover(&mut self, query: &DiscoveryQuery) -> Vec<DiscoveryResult> {
        self.total_queries += 1;
        let mut candidates: Vec<AgentDescriptor> = self.agents.values()
            .filter(|a| a.presence == Presence::Online || a.presence == Presence::Busy)
            .filter(|a| !query.exclude_agents.contains(&a.id))
            .filter(|a| a.trust_score >= query.min_trust)
            .filter(|a| a.match_score(&query.required_capabilities) >= 1.0)
            .cloned().collect();

        // Check if any required capabilities have no providers
        for req in &query.required_capabilities {
            if !self.agents.values().any(|a| a.capabilities.contains(req)) {
                // Can't satisfy, return empty
                return vec![];
            }
        }

        let mut results: Vec<DiscoveryResult> = candidates.iter().map(|agent| {
            let match_score = agent.match_score(&query.required_capabilities);
            let preferred_score = agent.match_score(&query.preferred_capabilities);
            let trust_bonus = agent.trust_score * 0.3;
            let composite = match_score * 0.5 + preferred_score * 0.2 + trust_bonus;
            DiscoveryResult { agent: agent.clone(), match_score, trust_bonus, composite_score: composite }
        }).collect();

        results.sort_by(|a, b| b.composite_score.partial_cmp(&a.composite_score).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(query.max_results);
        self.total_matches += results.len() as u64;
        results
    }

    /// Find agents with a specific capability
    pub fn find_by_capability(&self, capability: &str) -> Vec<&AgentDescriptor> {
        self.capability_index.get(capability).map(|ids| {
            ids.iter().filter_map(|id| self.agents.get(id)).filter(|a| a.presence != Presence::Offline).collect()
        }).unwrap_or_default()
    }

    /// Get all unique capabilities in the fleet
    pub fn all_capabilities(&self) -> Vec<&String> {
        let mut caps: Vec<&String> = self.capability_index.keys().filter(|k| !self.capability_index[k].is_empty()).collect();
        caps.sort();
        caps
    }

    /// Fleet statistics
    pub fn fleet_stats(&self) -> (usize, usize, usize) {
        let online = self.agents.values().filter(|a| a.presence == Presence::Online).count();
        let busy = self.agents.values().filter(|a| a.presence == Presence::Busy).count();
        let offline = self.agents.values().filter(|a| a.presence == Presence::Offline).count();
        (online, busy, offline)
    }

    /// Summary
    pub fn summary(&self) -> String {
        let (online, busy, offline) = self.fleet_stats();
        format!("Discovery: {}/{}/{} online/busy/offline, {} capabilities, {} queries, {} avg matches",
            online, busy, offline, self.all_capabilities().len(), self.total_queries,
            if self.total_queries > 0 { self.total_matches / self.total_queries } else { 0 })
    }
}

fn now() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_find() {
        let mut ds = DiscoverySystem::new();
        ds.register(AgentDescriptor::new("a1", "Navigator").with_capability("navigation").with_capability("pathfinding"));
        ds.register(AgentDescriptor::new("a2", "Sensor").with_capability("perception").with_capability("navigation"));
        let found = ds.find_by_capability("navigation");
        assert_eq!(found.len(), 2);
    }

    #[test]
    fn test_discover_with_requirements() {
        let mut ds = DiscoverySystem::new();
        ds.register(AgentDescriptor::new("a1", "Nav").with_capability("navigation"));
        ds.register(AgentDescriptor::new("a2", "Full").with_capability("navigation").with_capability("perception"));
        let query = DiscoveryQuery { required_capabilities: vec!["navigation".into(), "perception".into()], ..Default::default() };
        let results = ds.discover(&query);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].agent.id, "a2");
    }

    #[test]
    fn test_discover_excludes() {
        let mut ds = DiscoverySystem::new();
        ds.register(AgentDescriptor::new("a1", "Nav").with_capability("navigation"));
        ds.register(AgentDescriptor::new("a2", "Nav2").with_capability("navigation"));
        let mut query = DiscoveryQuery { required_capabilities: vec!["navigation".into()], ..Default::default() };
        query.exclude_agents.insert("a1".into());
        let results = ds.discover(&query);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_offline_excluded() {
        let mut ds = DiscoverySystem::new();
        ds.register(AgentDescriptor::new("a1", "Nav").with_capability("navigation"));
        ds.set_presence("a1", Presence::Offline);
        let found = ds.find_by_capability("navigation");
        assert_eq!(found.len(), 0);
    }

    #[test]
    fn test_deregister() {
        let mut ds = DiscoverySystem::new();
        ds.register(AgentDescriptor::new("a1", "X").with_capability("y"));
        ds.deregister("a1");
        assert_eq!(ds.find_by_capability("y").len(), 0);
    }

    #[test]
    fn test_all_capabilities() {
        let mut ds = DiscoverySystem::new();
        ds.register(AgentDescriptor::new("a1", "").with_capability("nav").with_capability("sense"));
        let caps = ds.all_capabilities();
        assert_eq!(caps.len(), 2);
    }

    #[test]
    fn test_min_trust_filter() {
        let mut ds = DiscoverySystem::new();
        let mut low = AgentDescriptor::new("a1", "Low");
        low.trust_score = 0.1;
        let mut high = AgentDescriptor::new("a2", "High");
        high.trust_score = 0.9;
        low.capabilities.push("nav".into());
        high.capabilities.push("nav".into());
        ds.register(low); ds.register(high);
        let query = DiscoveryQuery { required_capabilities: vec!["nav".into()], min_trust: 0.5, ..Default::default() };
        let results = ds.discover(&query);
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_fleet_stats() {
        let mut ds = DiscoverySystem::new();
        ds.register(AgentDescriptor::new("a1", ""));
        ds.register(AgentDescriptor::new("a2", ""));
        ds.set_presence("a2", Presence::Offline);
        let (online, _, offline) = ds.fleet_stats();
        assert_eq!(online, 1);
        assert_eq!(offline, 1);
    }

    #[test]
    fn test_summary() {
        let ds = DiscoverySystem::new();
        let s = ds.summary();
        assert!(s.contains("0 capabilities"));
    }
}
