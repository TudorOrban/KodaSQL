use sqlparser::ast::{ObjectName, TriggerEvent, TriggerExecBody, TriggerExecBodyType, TriggerPeriod};

use crate::{database::{database_loader, database_navigator::get_table_schema_path, types::{CustomIdent, TableSchema, Trigger, TriggerAction, TriggerEvent as CustomTriggerEvent, TriggerExecBody as CustomTriggerExecBody, TriggerExecBodyType as CustomTriggerExecBodyType, TriggerPeriod as CustomTriggerPeriod}}, shared::{errors::Error, file_manager}};

pub async fn create_trigger(name: &ObjectName, table_name: &ObjectName, period: &TriggerPeriod, events: &Vec<TriggerEvent>, exec_body: &TriggerExecBody) -> Result<String, Error> {
    let trigger = validate_create_trigger(name, table_name, period, events, exec_body)?;
    
    let database = database_loader::get_database()?;
    let schema_name = database.configuration.default_schema.clone();
    
    let table_schema_file_path = get_table_schema_path(&schema_name, &trigger.table_name);
    println!("table_schema_file_path: {:?}", table_schema_file_path);
    let mut table_schema = file_manager::read_json_file::<TableSchema>(&table_schema_file_path)?;

    table_schema.triggers.push(trigger);

    file_manager::write_json_into_file(&table_schema_file_path, &table_schema)?;

    Ok(String::from(""))
}

fn validate_create_trigger(name: &ObjectName, table_name: &ObjectName, period: &TriggerPeriod, events: &Vec<TriggerEvent>, exec_body: &TriggerExecBody) -> Result<Trigger, Error> {
    let first_trigger_identifier = name.0.first().ok_or(Error::MissingTriggerName)?;
    let trigger_name = first_trigger_identifier.value.clone();

    let first_table_identifier = table_name.0.first().ok_or(Error::MissingTableName)?;
    let table_name = first_table_identifier.value.clone();

    let trigger_period: CustomTriggerPeriod = match period {
        TriggerPeriod::Before => CustomTriggerPeriod::Before,
        TriggerPeriod::After => CustomTriggerPeriod::After,
        TriggerPeriod::InsteadOf => CustomTriggerPeriod::InsteadOf,
    };

    let events: Vec<CustomTriggerEvent> = events.iter().map(|event| match event {
        TriggerEvent::Insert => CustomTriggerEvent::Insert,
        TriggerEvent::Update(ids) => {
            let columns = ids.iter().map(|id| CustomIdent { value: id.value.clone(), quote_style: id.quote_style.clone() }).collect();
            CustomTriggerEvent::Update(columns)
        }
        TriggerEvent::Delete => CustomTriggerEvent::Delete,
        TriggerEvent::Truncate => CustomTriggerEvent::Truncate,
    }).collect();

    let exec_body = CustomTriggerExecBody {
        exec_type: match exec_body.exec_type {
            TriggerExecBodyType::Function => CustomTriggerExecBodyType::Function,
            TriggerExecBodyType::Procedure => CustomTriggerExecBodyType::Procedure,
        }
    };

    let trigger: Trigger = Trigger {
        name: trigger_name,
        table_name: table_name,
        period: trigger_period,
        events: events,
        action: TriggerAction { fuction_name: String::from("") },
        exec_body: exec_body
    };

    Ok(trigger)
}