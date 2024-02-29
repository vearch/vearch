# Vearch Backup Tool

## Overview

Vearch Backup Tool is a utility designed for creating backups of data stored in Vearch, an open-source distributed system for embedding-based retrieval. This tool facilitates the process of safely backing up and restoring collections and documents within a Vearch cluster, ensuring data durability and recoverability in case of data loss or system failures.

## Features

- **Data Backup**: Export spaces and documents from a running Vearch cluster.
- **Data Restore**: Import data back into a Vearch cluster, with options for selective restoration.
- **Incremental Backups**: Support for incremental backups to capture only the changes since the last backup.
- **Backup Compression**: Reduce the size of backup files with efficient compression algorithms.
- **Scheduled Backups**: Automate backup operations using cron jobs or other scheduling tools.
- **Backup Verification**: Ensure the integrity of backup files with checksum verification.

**Note**: Incremental backups are a planned feature and will be implemented in future releases.

## Prerequisites

Before using the Vearch Backup Tool, ensure that you have the following prerequisites met:

- A running Vearch cluster with access to the admin API.
- Sufficient disk space to store the backup files.
- (Optional) A scheduled job service like `cron` for automated backups.

## Installation

To install the Vearch Backup Tool, clone the repository and build the binary using the provided makefile:

```bash
git clone https://github.com/vearch/vearch.git
cd tools/vearch_backup
go build
```

## Usage

### Creating a Backup

To create a backup of your Vearch data, run the following command:

```bash
./vearch_backup --command=create -output /path/to/backup/folder
```

This will create a backup of all collections and documents in the specified output directory.

### Incremental Backup

To perform an incremental backup, use the `--incremental` flag:

```bash
./vearch_backup --command=create --output /path/to/backup/folder --incremental
```

### Restoring from Backup

To restore data from a backup, use the `restore` command:

```bash
./vearch_backup --command=restore --input /path/to/backup/folder
```

You can specify particular collections to restore with additional flags.

### Scheduling Backups

To schedule backups, you can use `cron` or any other scheduling tool. For example, to create a daily backup at midnight, add the following line to your crontab:

```bash
0 0 * * * /path/to/vearch_backup --command=create --output /path/to/backup/folder
```

## Configuration

The backup tool can be configured using command-line flags or a configuration file. See the `config.example.yaml` for a template.

## Troubleshooting

If you encounter issues, check the logs for error messages. Ensure that your Vearch cluster is accessible and that you have adequate permissions and disk space for the backup files.

## Contributing

Contributions to the Vearch Backup Tool are welcome! Please read the `CONTRIBUTING.md` file for guidelines on how to contribute.

## License

The Vearch Backup Tool is licensed under the [Apache License 2.0](LICENSE).

## Support

For support, please open an issue in the GitHub repository or contact the Vearch community through the official communication channels.

---

Please note that this is a generic template for a README file. You might need to adjust paths, URLs, and commands according to your actual backup tool's implementation details and repository structure.