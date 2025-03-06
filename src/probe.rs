/// 探测策略枚举
pub enum ProbeStrategy {
    Linear,
    Quadratic,
    DoubleHash,
}

/// 探测序列生成器
pub struct ProbeSequence {
    initial_pos: usize,
    current_step: usize,
    capacity: usize,
    strategy: ProbeStrategy,
    secondary_hash: usize,
}

impl ProbeSequence {
    /// 创建新的探测序列
    pub fn new(key: u64, capacity: usize, strategy: ProbeStrategy) -> Self {
        let initial_pos = (key as usize) % capacity;
        // 对于双重哈希，计算第二个哈希值
        let secondary_hash = match strategy {
            ProbeStrategy::DoubleHash => {
                // 一个简单的第二哈希函数，确保结果不为0
                let h2 = 1 + (key as usize % (capacity - 1));
                h2
            },
            _ => 0,
        };
        
        ProbeSequence {
            initial_pos,
            current_step: 0,
            capacity,
            strategy,
            secondary_hash,
        }
    }
    
    /// 获取下一个探测位置
    pub fn next(&mut self) -> usize {
        let pos = match self.strategy {
            ProbeStrategy::Linear => {
                (self.initial_pos + self.current_step) % self.capacity
            },
            ProbeStrategy::Quadratic => {
                (self.initial_pos + self.current_step + self.current_step * self.current_step) % self.capacity
            },
            ProbeStrategy::DoubleHash => {
                (self.initial_pos + self.current_step * self.secondary_hash) % self.capacity
            },
        };
        
        self.current_step += 1;
        pos
    }
}