use ordered_float::OrderedFloat;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashMap},
    iter,
};

pub type Hostname = &'static str;

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

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ChaosOp {
    Crash {
        start_ms: Option<u32>,
    },
    MsgLoss {
        start_ms: Option<u32>,
        stop_ms: Option<u32>,
        probability: OrderedFloat<f32>, // should change to f32, but issue with derive(Eq)
    },
    //? Add probabiltity field for this?
    //? duplication factor instead of rate? (confusing terms)
    MsgDuplication {
        start_ms: Option<u32>,
        stop_ms: Option<u32>,
        factor: i32,
        probability: OrderedFloat<f32>,
    },
}

impl ChaosOp {
    pub fn start_ms(&self) -> u32 {
        match self {
            ChaosOp::Crash { start_ms, .. }
            | ChaosOp::MsgLoss { start_ms, .. }
            | ChaosOp::MsgDuplication { start_ms, .. } => start_ms.unwrap_or(0),
        }
    }

    pub fn stop_ms(&self) -> Option<u32> {
        match self {
            ChaosOp::Crash { .. } => None,
            ChaosOp::MsgLoss { stop_ms, .. } | ChaosOp::MsgDuplication { stop_ms, .. } => *stop_ms,
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct PhysicalOp {
    pub nodename: String,
    pub actor_name: String,
    pub replicas: Option<u32>,
    //? Change to list (/map) of chaos ops for multiple choas ops per actor
    pub chaos: Option<Vec<ChaosOp>>,
    #[serde(flatten)]
    pub payload: HashMap<String, serde_json::Value>,
}

/// Takes logical Op  and places it on single or multiple Nodes. Returns list of Physical operator where a logical operator is placed
pub trait PlacementManager {
    fn place(&self, op_info: &LogicalOp) -> impl Iterator<Item = PhysicalOp>;
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ManualPlacementManager {
    pub map: HashMap<String, Vec<PhysicalOp>>,
}

impl ManualPlacementManager {
    pub fn new(map: HashMap<String, Vec<PhysicalOp>>) -> Self {
        let mut actual_placements: HashMap<String, Vec<PhysicalOp>> = HashMap::new();
        for (op, value) in map.into_iter() {
            let mut temp_vec: Vec<PhysicalOp> = Vec::new();
            for phys_op in value {
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
