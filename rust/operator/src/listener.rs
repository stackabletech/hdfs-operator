use stackable_hdfs_crd::ExposureConfig;

pub fn get_listener_volume(config: Option<ExposureConfig>) -> Option<Volume> {
    match config {
        None => None,
        Some(config) => Some(Volume {
            name: "listener".to_string(),
            ephemeral: Some(EphemeralVolumeSource {
                volume_claim_template: Some(PersistentVolumeClaimTemplate {
                    metadata: Some(ObjectMeta {
                        annotations: Some(
                            [(
                                "listeners.stackable.tech/listener-class".to_string(),
                                config.listener_class,
                            )]
                            .into(),
                        ),
                        ..Default::default()
                    }),
                    spec: PersistentVolumeClaimSpec {
                        access_modes: Some(vec!["ReadWriteMany".to_string()]),
                        resources: Some(ResourceRequirements {
                            requests: Some(
                                [("storage".to_string(), Quantity("1".to_string()))].into(),
                            ),
                            ..Default::default()
                        }),
                        storage_class_name: Some("listeners.stackable.tech".to_string()),
                        ..Default::default()
                    },
                }),
            }),
            ..Volume::default()
        }),
    }
}
