use rand::Rng;

#[derive(Debug)]
pub struct ChaosManager {
    pub msg_loss_probability: Option<f32>,
    pub msg_duplication_factor: Option<u32>,
    pub msg_duplication_probability: Option<f32>,
}

impl ChaosManager {
    pub fn new() -> Self {
        ChaosManager {
            msg_loss_probability: None,
            msg_duplication_factor: None,
            msg_duplication_probability: None,
        }
    }

    pub fn set_msg_loss(&mut self, probability: f32) {
        self.msg_loss_probability = Some(probability);
    }

    pub fn set_msg_duplication(&mut self, factor: u32, probability: f32) {
        self.msg_duplication_factor = Some(factor);
        self.msg_duplication_probability = Some(probability);
    }

    #[allow(dead_code)]
    pub fn unset_msg_loss(&mut self) {
        self.msg_loss_probability = None;
    }

    #[allow(dead_code)]
    pub fn unset_msg_duplication(&mut self) {
        self.msg_duplication_factor = None;
        self.msg_duplication_probability = None;
    }

    pub fn apply_chaos<T: Clone>(&self, msg: T) -> Vec<T> {
        let mut rng = rand::rng();

        if self
            .msg_loss_probability
            .is_some_and(|p| rng.random_bool(p as f64))
        {
            return vec![];
        }

        if let (Some(factor), Some(prob)) = (
            self.msg_duplication_factor,
            self.msg_duplication_probability,
        ) && rng.random_bool(prob as f64)
        {
            return vec![msg; factor as usize];
        }

        vec![msg]
    }
}
