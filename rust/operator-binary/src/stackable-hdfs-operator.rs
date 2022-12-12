use clap::{crate_description, crate_version, Parser};
use stackable_hdfs_crd::constants::APP_NAME;
use stackable_hdfs_crd::HdfsCluster;
use stackable_operator::{
    cli::{Command, ProductOperatorRun},
    client, CustomResourceExt,
};

mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
    pub const TARGET: Option<&str> = option_env!("TARGET");
}

#[derive(clap::Parser)]
#[clap(about, author)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    match opts.cmd {
        Command::Crd => HdfsCluster::print_yaml_schema()?,
        Command::Run(ProductOperatorRun {
            product_config,
            watch_namespace,
            tracing_target,
        }) => {
            stackable_operator::logging::initialize_logging(
                "HDFS_OPERATOR_LOG",
                APP_NAME,
                tracing_target,
            );

            stackable_operator::utils::print_startup_string(
                crate_description!(),
                crate_version!(),
                built_info::GIT_VERSION,
                built_info::TARGET.unwrap_or("unknown target"),
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
            );
            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/hdfs-operator/config-spec/properties.yaml",
            ])?;
            let client = client::create_client(Some("hdfs.stackable.tech".to_string())).await?;
            stackable_hdfs_operator::create_controller(client, product_config, watch_namespace)
                .await;
        }
    };

    Ok(())
}
