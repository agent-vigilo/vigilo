//! Scheduler installation contracts derived from the owning CREATE migrations.
//!
//! The SQL body remains defined once in the migration. Connection validation
//! compares catalog definitions against this binary's embedded source, rather
//! than trusting a function name or an operator-provided version label.

pub(super) struct Contract {
    pub signature: &'static str,
    migration: &'static str,
    pub control: bool,
    pub execution: bool,
}

macro_rules! contract {
    ($signature:literal, $migration:literal, $control:literal, $execution:literal) => {
        Contract {
            signature: $signature,
            migration: include_str!(concat!("../../../migrations/", $migration)),
            control: $control,
            execution: $execution,
        }
    };
}

pub(super) const CONTRACTS: &[Contract] = &[
    contract!(
        "vigilo_claim_dispatch(text[])",
        "0015_create_run_shard_dispatch_cursors.sql",
        true,
        false
    ),
    contract!(
        "vigilo_ready_admissions(integer)",
        "0011_create_run_plans.sql",
        true,
        false
    ),
    contract!(
        "vigilo_claim_admission(uuid,uuid)",
        "0011_create_run_plans.sql",
        true,
        false
    ),
    contract!(
        "vigilo_claim_control_deliveries(uuid)",
        "0037_create_run_control_deliveries.sql",
        true,
        false
    ),
    contract!(
        "vigilo_claim_revocation_settlements(uuid)",
        "0037_create_run_control_deliveries.sql",
        true,
        false
    ),
    contract!(
        "vigilo_claim_chunk_recovery(integer,bigint)",
        "0018_create_run_chunks.sql",
        false,
        true
    ),
    contract!(
        "vigilo_next_finalization_candidate(uuid[])",
        "0015_create_run_shard_dispatch_cursors.sql",
        true,
        false
    ),
    contract!(
        "vigilo_claim_finalization(uuid,uuid,integer)",
        "0010_create_runs.sql",
        true,
        false
    ),
    contract!(
        "vigilo_claim_outbox(bigint,integer)",
        "0029_create_outbox_delivery_queue.sql",
        true,
        true
    ),
    contract!(
        "vigilo_pending_aggregation(bigint)",
        "0026_create_run_shard_summaries.sql",
        false,
        true
    ),
    contract!(
        "vigilo_dispatch_backlog()",
        "0015_create_run_shard_dispatch_cursors.sql",
        true,
        false
    ),
    contract!(
        "vigilo_finalization_backlog()",
        "0015_create_run_shard_dispatch_cursors.sql",
        true,
        false
    ),
    contract!(
        "vigilo_outbox_backlog()",
        "0029_create_outbox_delivery_queue.sql",
        true,
        true
    ),
    contract!(
        "vigilo_execution_aliases()",
        "0014_create_shard_placements.sql",
        true,
        false
    ),
    contract!(
        "vigilo_unserviceable_routes()",
        "0014_create_shard_placements.sql",
        true,
        false
    ),
];

impl Contract {
    pub fn name(&self) -> &str {
        self.signature.split('(').next().expect("static signature")
    }

    pub fn definition(&self, quoted_schema: &str) -> Definition {
        let marker = format!("CREATE FUNCTION %1$I.{}(", self.name());
        let declaration = self
            .migration
            .split_once(&marker)
            .expect("embedded function")
            .1;
        let (header, body) = declaration
            .split_once("AS $function$")
            .expect("embedded body");
        let body = body.split_once("$function$;").expect("body end").0;
        let args = header.split_once(')').expect("arguments").0;
        let result = header
            .split_once("RETURNS ")
            .expect("return type")
            .1
            .split_once("LANGUAGE ")
            .expect("language")
            .0;
        let language = header
            .split_once("LANGUAGE ")
            .expect("language")
            .1
            .split_whitespace()
            .next()
            .expect("language name");
        Definition {
            body: body.replace("%1$I", quoted_schema).trim().to_string(),
            result: normalize(&result.replace("%1$I.", "")),
            arguments: args
                .split(',')
                .filter_map(|arg| arg.split_whitespace().next())
                .map(str::to_lowercase)
                .collect(),
            language: language.to_string(),
            volatility: if header.contains(" STABLE ") {
                "s"
            } else {
                "v"
            },
        }
    }
}

pub(super) struct Definition {
    pub body: String,
    pub result: String,
    pub arguments: Vec<String>,
    pub language: String,
    pub volatility: &'static str,
}

pub(super) fn normalize(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
        .replace("timestamp with time zone", "timestamptz")
        .replace(" (", "(")
        .replace("( ", "(")
        .replace(" )", ")")
        .replace(", ", ",")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn catalog_return_spelling_preserves_equivalent_postgres_types() {
        assert_eq!(
            normalize("TABLE (available_at TIMESTAMPTZ, id UUID)"),
            normalize("TABLE(available_at timestamp with time zone, id uuid)")
        );
        assert_ne!(normalize("TABLE(id uuid)"), normalize("TABLE(id text)"));
    }

    #[test]
    fn embedded_contracts_are_unique_complete_and_schema_bound() {
        let mut signatures = std::collections::BTreeSet::new();
        for contract in CONTRACTS {
            assert!(signatures.insert(contract.signature));
            let definition = contract.definition("test_schema");
            assert!(!definition.body.contains("%1$I"));
            assert!(definition.body.contains("test_schema."));
            assert!(!definition.result.is_empty());
            assert!(matches!(definition.language.as_str(), "sql" | "plpgsql"));
        }
        assert_eq!(signatures.len(), 15);
    }
}
