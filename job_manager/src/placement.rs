use serde::{Deserialize, Deserializer};
use std::{
    collections::{BTreeMap, HashMap},
    iter,
};

pub type Hostname = &'static str;

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Probability(pub f32);

impl<'de> Deserialize<'de> for Probability {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = f32::deserialize(deserializer)?;
        if (0.0..=1.0).contains(&v) {
            Ok(Probability(v))
        } else {
            Err(serde::de::Error::custom(format!(
                "probability must be between 0.0 and 1.0, got {v}"
            )))
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct Factor(pub i32);

impl<'de> Deserialize<'de> for Factor {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let v = i32::deserialize(deserializer)?;
        if v >= 1 {
            Ok(Factor(v))
        } else {
            Err(serde::de::Error::custom(format!(
                "factor must be >= 1, got {v}"
            )))
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct NodeInfo {
    pub name: String,
    pub hostname: String,
    pub port: u16,
}

pub struct Placement {
    hostname_to_num: BTreeMap<&'static str, u32>,
}

impl Placement {
    pub fn num(&self) -> u32 {
        self.hostname_to_num.values().sum::<u32>()
    }
    pub fn iter(&self) -> impl Iterator<Item = Hostname> + '_ {
        self.hostname_to_num
            .iter()
            .flat_map(|(hostname, &count)| iter::repeat_n(*hostname, count as usize))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LibInfo {
    pub name: String,
    pub compile_info: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct LogicalOp {
    pub name: String,
    pub lib_name: String,
}

#[derive(Debug, Copy, Clone, Deserialize, PartialEq)]
pub struct CrashOp {
    pub crash_ms: Option<u32>,
    pub restart_ms: Option<u32>,
}

#[derive(Debug, Copy, Clone, Deserialize, PartialEq)]
pub struct MsgLossOp {
    pub start_ms: Option<u32>,
    pub stop_ms: Option<u32>,
    pub probability: Probability,
}

#[derive(Debug, Copy, Clone, Deserialize, PartialEq)]
pub struct MsgDuplicationOp {
    pub start_ms: Option<u32>,
    pub stop_ms: Option<u32>,
    pub factor: Factor,
    pub probability: Probability,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct MsgDelayOp {
    pub start_ms: Option<u32>,
    pub stop_ms: Option<u32>,
    // Both inclusive
    pub delay_range_ms: (u64, u64),
    pub senders: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ChaosMap {
    pub crash: Option<CrashOp>,
    pub msg_loss: Option<MsgLossOp>,
    pub msg_duplication: Option<MsgDuplicationOp>,
    pub msg_delay: Option<MsgDelayOp>,
}

impl ChaosMap {
    /// Return an iterator over all chaos ops present
    pub fn iter(&self) -> impl Iterator<Item = ChaosOp> {
        let mut v = Vec::new();
        if let Some(op) = &self.msg_loss {
            v.push(ChaosOp::MsgLoss(*op));
        }
        if let Some(op) = &self.msg_duplication {
            v.push(ChaosOp::MsgDuplication(*op));
        }
        if let Some(op) = &self.crash {
            v.push(ChaosOp::Crash(*op));
        }
        if let Some(op) = &self.msg_delay {
            v.push(ChaosOp::MsgDelay(op.clone()));
        }
        v.into_iter()
    }

    /// Create a new ChaosMap by extending the current one with another one prioritizing the current values in case of conflict
    pub fn extend(self, other: Self) -> Self {
        Self {
            crash: self.crash.or(other.crash),
            msg_loss: self.msg_loss.or(other.msg_loss),
            msg_duplication: self.msg_duplication.or(other.msg_duplication),
            msg_delay: self.msg_delay.or(other.msg_delay),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ChaosOp {
    Crash(CrashOp),
    MsgLoss(MsgLossOp),
    MsgDuplication(MsgDuplicationOp),
    MsgDelay(MsgDelayOp),
}

impl ChaosOp {
    pub fn start_ms(&self) -> u32 {
        match self {
            Self::Crash(op) => op.crash_ms.unwrap_or(0),
            Self::MsgLoss(op) => op.start_ms.unwrap_or(0),
            Self::MsgDuplication(op) => op.start_ms.unwrap_or(0),
            Self::MsgDelay(op) => op.start_ms.unwrap_or(0),
        }
    }

    pub fn stop_ms(&self) -> Option<u32> {
        match self {
            Self::Crash(op) => op.restart_ms,
            Self::MsgLoss(op) => op.stop_ms,
            Self::MsgDuplication(op) => op.stop_ms,
            Self::MsgDelay(op) => op.stop_ms,
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct PhysicalOp {
    pub nodename: String,
    pub actor_name: String,
    pub replicas: Option<u32>,
    pub chaos: Option<ChaosMap>,
    #[serde(flatten)]
    pub payload: HashMap<String, serde_json::Value>,
}

/// Takes logical Op  and places it on single or multiple Nodes. Returns list of Physical operator where a logical operator is placed
pub trait PlacementManager {
    fn place(&self, op_info: &LogicalOp) -> impl Iterator<Item = PhysicalOp>;
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct ManualPlacementManager {
    pub map: HashMap<String, Vec<PhysicalOp>>,
}

impl ManualPlacementManager {
    pub fn new(
        placement_map: HashMap<String, Vec<PhysicalOp>>,
        gchaos_map: Option<ChaosMap>,
    ) -> Self {
        let mut actual_placements: HashMap<String, Vec<PhysicalOp>> = HashMap::new();
        for (op, value) in placement_map.into_iter() {
            let mut temp_vec: Vec<PhysicalOp> = Vec::new();
            for mut phys_op in value {
                // phys_op.chaos = phys_op.chaos.extend(gchaos_map.clone().unwrap_or(ChaosMap {
                //     crash: None,
                //     msg_loss: None,
                //     msg_duplication: None,
                // }));
                phys_op.chaos = match (phys_op.chaos, gchaos_map.clone()) {
                    (Some(p_chaos), Some(g_chaos)) => Some(p_chaos.extend(g_chaos)),
                    (Some(p_chaos), None) => Some(p_chaos),
                    (None, Some(g_chaos)) => Some(g_chaos),
                    (None, None) => None,
                };
                if let Some(replicas) = phys_op.replicas {
                    for i in 1..=replicas {
                        temp_vec.push(PhysicalOp {
                            nodename: phys_op.nodename.clone(),
                            actor_name: format!("{}{}", phys_op.actor_name, i),
                            payload: phys_op.payload.clone(),
                            replicas: None,
                            chaos: phys_op.chaos.clone(),
                        });
                    }
                } else {
                    temp_vec.push(phys_op);
                }
            }
            actual_placements.insert(op.clone(), temp_vec);
        }

        Self {
            map: actual_placements,
        }
    }
}

impl PlacementManager for ManualPlacementManager {
    fn place(&self, op_info: &LogicalOp) -> impl Iterator<Item = PhysicalOp> {
        self.map.get(&op_info.name).unwrap().iter().cloned()
    }
}
