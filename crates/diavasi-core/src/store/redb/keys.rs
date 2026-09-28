use ::redb::TableDefinition;

pub const SCHEMA_VERSION: u32 = 1;
pub const SCHEMA_VERSION_KEY: &str = "schema_version";

pub const META: TableDefinition<'_, &str, u32> = TableDefinition::new("meta");
pub const CONNECTIONS: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("connections");
pub const GROUPS: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("groups");
pub const CHECKPOINTS: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("checkpoints");
