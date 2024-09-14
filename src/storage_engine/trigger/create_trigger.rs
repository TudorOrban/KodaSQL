use sqlparser::ast::{ObjectName, TriggerEvent, TriggerExecBody, TriggerPeriod};

use crate::{database::types::{Trigger, TriggerAction, TriggerPeriod as CustomTriggerPeriod}, shared::errors::Error};

pub async fn create_trigger(name: &ObjectName, table_name: &ObjectName, period: &TriggerPeriod, events: &Vec<TriggerEvent>, exec_body: &TriggerExecBody) -> Result<String, Error> {
    let first_trigger_identifier = name.0.first().ok_or(Error::MissingTriggerName)?;
    let trigger_name = first_trigger_identifier.value.clone();

    let first_table_identifier = table_name.0.first().ok_or(Error::MissingTableName)?;
    let table_name = first_table_identifier.value.clone();

    let trigger_period: CustomTriggerPeriod = match period {
        TriggerPeriod::Before => CustomTriggerPeriod::Before,
        TriggerPeriod::After => CustomTriggerPeriod::After,
        TriggerPeriod::InsteadOf => CustomTriggerPeriod::InsteadOf,
    };

    let trigger: Trigger = Trigger {
        name: trigger_name,
        table_name: table_name,
        period: trigger_period,
        events: Vec::new(),
        action: TriggerAction { fuction_name: String::from("") }
    };

    Ok(String::from(""))
}