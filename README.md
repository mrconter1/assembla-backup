# assembla-backup

Create a **complete, offline copy of your Assembla space(s)** - tickets, comments, milestones, wiki (with full history), attachments, users, and complete **git repo history** - captured via the Assembla API and `git clone --mirror`, then bundled into a single timestamped zip.

Unlike the partial exporters out there, this aims for *everything in one archive*: project-management data **and** the repositories.

## What it backs up

- **Spaces** - all spaces you can access (or an allow-list)
- **Tickets** - every ticket, with **comments**, statuses, custom fields, tags, associations
- **Milestones** - open and closed
- **Wiki** - pages plus full version history
- **Documents / attachments** - metadata **and** the downloaded file bytes
- **Users & roles** - so authors/assignees are readable
- **Git repositories** - full history, all branches and tags (bare `--mirror` clones)

Everything the API returns is saved as **raw JSON** (faithful and re-importable), not reshaped.

## Requirements

- **Python 3.10+** with `requests` and `python-dotenv` (`pip install -r requirements.txt`)
- **`git`** on your PATH (used for the repository clones)

## Setup

1. In Assembla, go to **My Profile → API Applications & Sessions** and **Register a new personal key** with **both**:
   - *API access* ✔ (for the REST API)
   - *Repository access* ✔ (the key secret doubles as your Git-over-HTTPS password)
2. Copy `.env.example` to `.env` and fill in:
   ```
   ASSEMBLA_API_KEY=your_key
   ASSEMBLA_API_SECRET=your_secret
   ```

The API is called with headers `X-Api-Key` / `X-Api-Secret` against `https://api.assembla.com/v1/`. Repositories are cloned via `https://<key>:<secret>@git.assembla.com/<repo>.git`.

## Usage

```bash
# Back up every space you can access, into a zip
python assembla_backup.py

# Only specific spaces
python assembla_backup.py --spaces rektron-eyesight rektron-netgauge

# Keep the folder, skip the zip
python assembla_backup.py --no-zip
```

### Flags

| Flag | Default | Effect |
|------|---------|--------|
| `--spaces <name...>` | all accessible spaces | restrict to specific spaces |
| `--no-zip` | off (zip is created) | keep the output folder, skip zipping |
| `--out <dir>` | `./` | where the backup folder/zip is written |

## Output structure

```
assembla-backup-<timestamp>/
├── manifest.json                 # what was backed up, counts, tool version
├── spaces.json
└── spaces/<space>/
    ├── space.json  space_tools.json  users.json  user_roles.json
    ├── tickets/
    │   ├── _index.json  statuses.json  custom_fields.json  tags.json
    │   └── comments/<ticket_number>.json
    ├── milestones.json
    ├── wiki/_index.json  wiki/versions/<page_id>.json
    ├── documents/_index.json  documents/files/<doc_id>__<filename>
    └── repos/<repo_name>.git/     # bare --mirror clone
→ zipped to assembla-backup-<timestamp>.zip
```

## Fail-fast by design

The tool **stops on the first unexpected error** - any `401/403/unexpected 404/5xx` raises and halts. It never silently skips data. The final `.zip` is written **only after every step of every space succeeds**, so a zip always means "verified complete." A failed run leaves the working folder for inspection but produces **no zip**.

Rate-limit responses (`429`/`503`) are the one exception: they are retried with backoff a few times before being treated as a failure.

## Not supported

- **SVN repositories.** If a space contains an SVN tool, the script **stops** and reports it - git only for now.

## Security

A backup contains everything in your spaces, including private tickets, attachments, and full repo history. Treat the output as sensitive: keep it off shared drives and **never commit a backup or your `.env`** to git.
