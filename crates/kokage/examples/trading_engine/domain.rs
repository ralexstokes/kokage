//! Small domain types shared by the actors in this example.

#[derive(Clone, Debug)]
pub struct Quote {
    pub symbol: &'static str,
    pub bid: u64,
    pub ask: u64,
}

#[derive(Clone, Debug)]
pub struct Order {
    pub id: u64,
    pub symbol: &'static str,
    pub quantity: i64,
    pub limit: u64,
}

#[derive(Clone, Debug)]
pub struct Fill {
    pub order_id: u64,
    pub symbol: &'static str,
    pub quantity: i64,
    pub price: u64,
}
