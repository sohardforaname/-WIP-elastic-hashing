mod bucket;
use bucket::ElasticHashing;

fn main() {
    let hash = ElasticHashing::new(10);
    println!("Created elastic hashing with size {}", hash.size);
}
