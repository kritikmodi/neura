use anyhow::Result;
use clap::{Parser, Subcommand};
use futures::StreamExt;
use neura::{config::Config, engine::Engine};
use std::sync::Arc;
#[derive(Parser)]
#[command(version, about = "Read-only federated SQL over governed data sources")]
struct Args {
    #[arg(short, long, default_value = "neura.toml")]
    config: String,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    Serve,
    Query {
        sql: String,
        #[arg(long)]
        diagnostics: bool,
    },
    CheckConfig,
}
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "neura=info".into()),
        )
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();
    let config = Config::load(&args.config)?;
    if matches!(args.command, Command::CheckConfig) {
        println!("configuration valid (connections not tested)");
        return Ok(());
    }
    let engine = Engine::new(config)?;
    match args.command {
        Command::Serve => {
            tracing::info!(http=%engine.config.server.http_addr,"Neura REST listening");
            tokio::select! {r=serve(engine)=>r?,_=tokio::signal::ctrl_c()=>{}}
        }
        Command::Query { sql, diagnostics } => {
            let mut q = engine.query(&sql).await?;
            println!(
                "{}",
                serde_json::json!({"type":"schema","id":q.id,"columns":q.schema.fields().iter().map(|f|serde_json::json!({"name":f.name(),"type":f.data_type().to_string()})).collect::<Vec<_>>()})
            );
            while let Some(batch) = q.stream.next().await {
                let batch = batch?;
                for i in 0..batch.num_rows() {
                    println!(
                        "{}",
                        serde_json::json!({"type":"row","values":neura::api::row(&batch,i)?})
                    );
                }
            }
            println!("{}", serde_json::json!({"type":"complete","id":q.id}));
            if diagnostics {
                eprintln!(
                    "{}",
                    serde_json::to_string_pretty(&engine.diagnostic(&q.id))?
                );
            }
        }
        Command::CheckConfig => {}
    }
    Ok(())
}
async fn serve(e: Arc<Engine>) -> Result<()> {
    #[cfg(feature = "postgres-wire")]
    if e.config.server.pg_addr.is_some() {
        tracing::info!(pg=?e.config.server.pg_addr,"Optional PostgreSQL compatibility listener enabled");
        tokio::try_join!(neura::api::serve(e.clone()), neura::pgwire::serve(e))?;
        return Ok(());
    }
    neura::api::serve(e).await
}
