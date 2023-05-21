use crate::{
    config::Config,
    record::Writer,
    storage::{FileType, Storage},
    version::Version,
    Result,
};
use std::path::{Path, PathBuf};
use log::warn;

pub(crate) fn generate_file_name(
    path: impl AsRef<Path>,
    file_num: u64,
    file_type: FileType,
) -> PathBuf {
    let name = match file_type {
        FileType::Log => format!("{file_num}.log"),
        FileType::SSTable => format!("{file_num}.sst"),
        FileType::Inventory => "inventory".to_owned(),
        FileType::InventoryTmp => "inventory.tmp".to_owned(),
        FileType::Unknown => {
            warn!("generate filename for unknown type file");
            "unknown".to_owned()
        }
    };

    Path::new(path.as_ref()).join(name)
}

pub(crate) fn create_wal_writer<S: Storage>(
    path: impl AsRef<Path>,
    storage: &S,
    version: &mut Version,
    config: Config,
) -> Result<Writer<S::F>> {
    let wal_log_num = version.allocate_next_file_num();
    version.log_number = wal_log_num;
    let wal_filename = Path::new(path.as_ref()).join(format!("{wal_log_num}.log"));
    let wal_f = storage.create(wal_filename)?;
    let wal_writer = Writer::new(wal_f, config);
    Ok(wal_writer)
}

pub(crate) fn parse_filename(path: impl AsRef<Path>) -> (u64, FileType) {
    let base = path
        .as_ref()
        .file_stem()
        .map(|s| s.to_str().unwrap().to_owned());
    let ext = path
        .as_ref()
        .extension()
        .map(|s| s.to_str().unwrap().to_owned());

    match (base, ext) {
        (Some(base), Some(ext)) if base == "inventory" && ext == "tmp" => {
            (0, FileType::InventoryTmp)
        }
        (Some(base), Some(ext)) => {
            let file_type = match ext.as_str() {
                "log" => FileType::Log,
                "sst" => FileType::SSTable,
                _ => FileType::Unknown,
            };
            match base.parse::<u64>() {
                Ok(filenum) => (filenum, file_type),
                Err(_) => (0, FileType::Unknown),
            }
        }
        (Some(base), None) if base == "inventory" => (0, FileType::Inventory),
        _ => return (0, FileType::Unknown),
    }
}
