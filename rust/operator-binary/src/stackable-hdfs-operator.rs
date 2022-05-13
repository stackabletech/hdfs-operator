use clap::Parser;
use stackable_hdfs_crd::HdfsCluster;
use stackable_hdfs_operator::ControllerConfig;
use stackable_operator::cli::{Command, ProductOperatorRun};
use stackable_operator::kube::CustomResourceExt;
use stackable_operator::{client, error};

mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[derive(clap::Parser)]
#[clap(about = built_info::PKG_DESCRIPTION, author = stackable_operator::cli::AUTHOR)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command<HdfsRun>,
}

#[derive(clap::Parser)]
struct HdfsRun {
    #[clap(long, env)]
    hdfs_datanode_clusterrole: String,
    #[clap(flatten)]
    common: ProductOperatorRun,
}

#[tokio::main]
async fn main() -> Result<(), error::Error> {
    stackable_operator::logging::initialize_logging("HDFS_OPERATOR_LOG");

    let opts = Opts::parse();
    match opts.cmd {
        Command::Crd => println!("{}", serde_yaml::to_string(&HdfsCluster::crd())?),
        Command::Run(HdfsRun {
            hdfs_datanode_clusterrole,
            common:
                ProductOperatorRun {
                    product_config,
                    watch_namespace,
                },
        }) => {
            stackable_operator::utils::print_startup_string(
                built_info::PKG_DESCRIPTION,
                built_info::PKG_VERSION,
                built_info::GIT_VERSION,
                built_info::TARGET,
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
            );
            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/hdfs-operator/config-spec/properties.yaml",
            ])?;
            let client = client::create_client(Some("hdfs.stackable.tech".to_string())).await?;
            stackable_hdfs_operator::create_controller(
                client,
                ControllerConfig {
                    datanode_clusterrole: hdfs_datanode_clusterrole,
                },
                product_config,
                watch_namespace,
            )
            .await;
        }
    };

    Ok(())
}
