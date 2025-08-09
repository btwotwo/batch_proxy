mod batch;
mod request;
mod api_client;



#[derive(Debug, PartialEq, Eq, Hash)]
struct ParameterSet {
    normalize: Option<bool>,
    prompt_name: Option<String>,
    truncate: Option<bool>,
    truncation_direction: Option<String>
}


fn main() {
    println!("Hello, world!");
}
