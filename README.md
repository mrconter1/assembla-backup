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
- **Repositories** - Git (full history via bare `--mirror` clone) and SVN (full history via `svnrdump` dump)

Everything the API returns is saved as **raw JSON** (faithful and re-importable), not reshaped.

## Requirements

- **Python 3.10+** with `requests` and `python-dotenv` (`pip install -r requirements.txt`)
- **`git`** on your PATH (for Git repositories)
- **`svnrdump`** on your PATH (only if a space has an SVN repo; ships with Subversion)

## Setup

1. In Assembla, go to **My Profile → API Applications & Sessions** and **Register a new personal key** with **both**:
   - *API access* ✔ (for the REST API)
   - *Repository access* ✔ (the key secret doubles as your Git-over-HTTPS password)
2. Copy `.env.example` to `.env` and fill in:
   ```
   ASSEMBLA_API_KEY=your_key
   ASSEMBLA_API_SECRET=your_secret
   # Only needed if a space has an SVN repo:
   ASSEMBLA_USERNAME=your_assembla_login
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
| `--workers <n>` | `8` | concurrent requests for comments/files/wiki |
| `--strict-files` | off | make any file-download failure fatal (see below) |
| `--list-spaces` | - | list spaces the key can access, then exit |

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
    └── repos/
        ├── <repo_name>.git/       # git: bare --mirror clone
        └── <repo_name>.svndump    # svn: full svnrdump dump (reload with `svnadmin load`)
→ zipped to assembla-backup-<timestamp>.zip
```

## Fail-fast by design

The tool **stops on the first unexpected error** - any `401/403/unexpected 404/5xx` raises and halts. It never silently skips data. The final `.zip` is written **only after every step of every space succeeds**, so a zip always means "verified complete." A failed run leaves the working folder for inspection but produces **no zip**.

Rate-limit responses (`429`/`503`) are retried with backoff (honoring `Retry-After`, with jitter) before being treated as a failure.

**One deliberate exception: individual file downloads.** Assembla occasionally hands out broken presigned S3 URLs (e.g. a signature signed for the wrong region), so a specific attachment can be un-downloadable through no fault of yours. Rather than let one dead blob abort a multi-space backup, such a file is **recorded and skipped**, listed under `failed_downloads` in `manifest.json`, and the final message clearly says "complete EXCEPT N unreachable file(s)" instead of "verified complete". Pass `--strict-files` to make these fatal instead.

## Restoring a repository

- **Git:** `git clone <repo_name>.git restored` (it is a normal bare mirror).
- **SVN:** `svnadmin create restored && svnadmin load restored < <repo_name>.svndump`.

## Security

A backup contains everything in your spaces, including private tickets, attachments, and full repo history. Treat the output as sensitive: keep it off shared drives and **never commit a backup or your `.env`** to git.
