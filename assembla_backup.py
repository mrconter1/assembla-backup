#!/usr/bin/env python3
"""
assembla-backup: create a complete offline copy of your Assembla space(s).

Backs up (via the Assembla API v1): spaces, tickets + comments, ticket
statuses / custom fields / tags, milestones, wiki pages + versions, documents
+ their file bytes, users and roles. Repositories are captured with
`git clone --mirror` (full history, all branches and tags).

Fail-fast: any unexpected error stops the run immediately. The final .zip is
only written after every step of every space succeeds, so a zip always means
"verified complete". SVN repositories are not supported and stop the run.
"""

import argparse
import json
import os
import random
import shutil
import subprocess
import sys
import threading
import time
from collections import deque
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter
from dotenv import load_dotenv

API_BASE = "https://api.assembla.com/v1"
GIT_HOST = "git.assembla.com"
PER_PAGE = 100
MAX_RATE_LIMIT_RETRIES = 5
CONN_POOL = 32  # HTTP connection pool size (>= max --workers you intend to use)
MAX_LEAF = 110  # cap on a saved filename length (Windows MAX_PATH safety)
# (connect timeout, read timeout) seconds. Read timeout is per-chunk, so large
# downloads are fine; it only trips when the server stops responding.
TIMEOUT = (10, 60)

# ANSI colours (work in modern Windows Terminal and most shells)
RED = "\033[31m"
GREEN = "\033[32m"
CYAN = "\033[36m"
YELLOW = "\033[33m"
DIM = "\033[2m"
RESET = "\033[0m"


def step(msg):
    print(f"{CYAN}==> {msg}{RESET}")


def ok(msg):
    print(f"{GREEN}    {msg}{RESET}")


def info(msg):
    print(f"{DIM}    {msg}{RESET}")


def fail(msg):
    print(f"{RED}ERROR: {msg}{RESET}", file=sys.stderr)


class BackupError(Exception):
    """Any condition that must halt the whole backup."""


class DownloadError(BackupError):
    """A single file blob could not be downloaded (often an Assembla-side S3
    issue). Tolerated by default and recorded, unless --strict-files is set."""


class AssemblaClient:
    def __init__(self, key, secret):
        self._key = key
        self._secret = secret
        self.failed_downloads = []
        self._fail_lock = threading.Lock()
        self.session = requests.Session()
        self.session.headers.update(
            {
                "X-Api-Key": key,
                "X-Api-Secret": secret,
                "Accept": "application/json",
            }
        )
        # Size the connection pool for concurrency so extra workers do not
        # thrash new connections on every request.
        adapter = HTTPAdapter(pool_connections=CONN_POOL, pool_maxsize=CONN_POOL)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def get(self, path, params=None, *, allow_404=False):
        """GET {API_BASE}/{path}. Fail-fast on unexpected status.

        Returns parsed JSON, or None when allow_404 and the resource is 404.
        Retries transient connection errors and 429/503, then fails.
        """
        url = f"{API_BASE}/{path}"
        attempt = 0
        while True:
            try:
                resp = self.session.get(url, params=params, timeout=TIMEOUT)
            except requests.exceptions.RequestException as exc:
                attempt = self._sleep_retry(attempt, path, f"connection error ({exc.__class__.__name__})")
                if attempt is None:
                    raise BackupError(f"GET {path} failed after retries: {exc}")
                continue
            if resp.status_code in (429, 503):
                attempt = self._sleep_retry(attempt, path, f"rate limited ({resp.status_code})",
                                            resp.headers.get("Retry-After"))
                if attempt is None:
                    raise BackupError(f"Rate limited on {path} after {MAX_RATE_LIMIT_RETRIES} retries")
                continue
            if resp.status_code == 404 and allow_404:
                return None
            if not resp.ok:
                raise BackupError(
                    f"GET {path} returned {resp.status_code}: {resp.text[:300]}"
                )
            if not resp.content:
                return []
            try:
                return resp.json()
            except ValueError as exc:
                raise BackupError(f"GET {path} returned non-JSON body: {exc}")

    def paginate(self, path, params=None, optional=False):
        """Yield every item across all pages of a list endpoint.

        Self-terminating: stops on a short/empty page, and also if a page
        contains no items we have not already seen (guards against endpoints
        that ignore page/per_page and return the whole list every time).

        optional=True treats a 404 (e.g. "Tool not found for this project",
        when a space simply lacks that tool) as an empty result.
        """
        params = dict(params or {})
        params["per_page"] = PER_PAGE
        page = 1
        seen = set()
        while True:
            params["page"] = page
            batch = self.get(path, params=params, allow_404=optional)
            if not batch:  # None (optional 404) or empty page
                return
            if not isinstance(batch, list):
                raise BackupError(f"Expected a list from {path}, got {type(batch).__name__}")
            new = 0
            for item in batch:
                key = item.get("id") or item.get("number") if isinstance(item, dict) else None
                if key is None:
                    key = repr(item)
                if key in seen:
                    continue
                seen.add(key)
                new += 1
                yield item
            if len(batch) < PER_PAGE or new == 0:
                return
            page += 1

    def download(self, path, dest: Path):
        """Stream a binary download endpoint to a file.

        Retries transient connection errors and 429/503. A persistent failure
        raises DownloadError (recorded and skipped unless --strict-files).
        """
        url = f"{API_BASE}/{path}"
        attempt = 0
        while True:
            try:
                with self.session.get(url, stream=True, timeout=TIMEOUT) as resp:
                    if resp.status_code in (429, 503):
                        attempt = self._sleep_retry(attempt, path, f"rate limited ({resp.status_code})",
                                                    resp.headers.get("Retry-After"))
                        if attempt is None:
                            raise DownloadError(f"{path} rate limited after retries")
                        continue
                    if not resp.ok:
                        raise DownloadError(
                            f"{path} returned {resp.status_code}: {resp.text[:160].strip()}"
                        )
                    dest.parent.mkdir(parents=True, exist_ok=True)
                    with open(dest, "wb") as fh:
                        for chunk in resp.iter_content(chunk_size=65536):
                            if chunk:
                                fh.write(chunk)
                    return
            except requests.exceptions.RequestException as exc:
                attempt = self._sleep_retry(attempt, path, f"connection error ({exc.__class__.__name__})")
                if attempt is None:
                    raise DownloadError(f"{path} failed after retries: {exc}")
                continue

    def _sleep_retry(self, attempt, path, reason, retry_after=None):
        """Sleep before a retry; return the new attempt count, or None if the
        retry budget is exhausted.

        Honors a numeric Retry-After header, otherwise exponential backoff, plus
        jitter so concurrent workers do not retry in lockstep.
        """
        attempt += 1
        if attempt > MAX_RATE_LIMIT_RETRIES:
            return None
        if retry_after and retry_after.strip().isdigit():
            wait = float(retry_after)
        else:
            wait = min(60.0, 2.0 ** attempt)
        wait += random.uniform(0, 1)  # jitter
        info(f"{reason} on {path}, retry {attempt}/{MAX_RATE_LIMIT_RETRIES} in {wait:.1f}s")
        time.sleep(wait)
        return attempt

    def git_url(self, repo_path):
        """HTTPS clone URL with credentials embedded (secret is never logged)."""
        return f"https://{quote(self._key)}:{quote(self._secret)}@{GIT_HOST}/{repo_path}"


def parallel_map(fn, items, workers, label=None):
    """Run fn(item) over items with a bounded thread pool, preserving order.

    Fail-fast: the first task to raise propagates out (remaining tasks are
    cancelled), so a partial result never masquerades as complete.
    """
    if not items:
        return []
    results = [None] * len(items)
    total = len(items)
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fn, item): idx for idx, item in enumerate(items)}
        try:
            for fut in as_completed(futs):
                results[futs[fut]] = fut.result()  # re-raises task errors here
                done += 1
                if label and (done % 20 == 0 or done == total):
                    info(f"  {label} {done}/{total}")
        except BaseException:
            ex.shutdown(wait=False, cancel_futures=True)
            raise
    return results


def write_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, ensure_ascii=False)


def safe_name(value):
    """Filesystem-safe folder/file name."""
    keep = "-_.() "
    return "".join(c if c.isalnum() or c in keep else "_" for c in str(value)).strip() or "unnamed"


def capped_leaf(doc_id, fname):
    """A filesystem-safe, length-capped leaf name for a downloaded document.

    Keeps the doc_id prefix (guarantees uniqueness) and preserves the file
    extension, truncating only the human-readable part so the full path stays
    under the Windows MAX_PATH limit.
    """
    name = safe_name(fname)
    if len(name) > MAX_LEAF:
        stem, dot, ext = name.rpartition(".")
        if dot and 0 < len(ext) <= 8:
            name = stem[:MAX_LEAF - len(ext) - 1] + "." + ext
        else:
            name = name[:MAX_LEAF]
    return f"{doc_id}__{name}"


def is_svn_tool(tool):
    t = f"{tool.get('type', '')} {tool.get('name', '')} {tool.get('menu_name', '')}".lower()
    return "subversion" in t or "svn" in t


def is_git_tool(tool):
    t = f"{tool.get('type', '')} {tool.get('name', '')} {tool.get('menu_name', '')}".lower()
    return "git" in t


def repo_path_from_url(url):
    """Extract the repo path (e.g. 'space-name.git') from a GitTool url.

    Handles the SSH form 'git@git.assembla.com:<path>' and the HTTPS form
    'https://git.assembla.com/<path>'.
    """
    if not url:
        return None
    url = url.strip()
    if url.startswith("git@"):
        return url.split(":", 1)[1] if ":" in url else None
    marker = f"{GIT_HOST}/"
    if marker in url:
        return url.split(marker, 1)[1]
    return None


def backup_space(client: AssemblaClient, space, root: Path, workers: int,
                 strict_files: bool, username):
    space_id = space["id"]
    wiki_name = space.get("wiki_name") or space_id
    sdir = root / "spaces" / safe_name(wiki_name)
    step(f"Space: {space.get('name', wiki_name)}  ({wiki_name})")

    write_json(sdir / "space.json", space)

    # Tools (discover repos, and detect SVN early).
    info("fetching space tools")
    tools = client.get(f"spaces/{space_id}/space_tools") or []
    write_json(sdir / "space_tools.json", tools)

    # Users and roles.
    info("fetching users and roles")
    write_json(sdir / "users.json", client.get(f"spaces/{space_id}/users") or [])
    write_json(sdir / "user_roles.json", client.get(f"spaces/{space_id}/user_roles") or [])

    # Ticket schema.
    info("fetching ticket schema (statuses, custom fields, tags)")
    write_json(sdir / "tickets" / "statuses.json",
               client.get(f"spaces/{space_id}/tickets/statuses", allow_404=True) or [])
    write_json(sdir / "tickets" / "custom_fields.json",
               client.get(f"spaces/{space_id}/tickets/custom_fields", allow_404=True) or [])
    write_json(sdir / "tickets" / "tags.json",
               client.get(f"spaces/{space_id}/tags", allow_404=True) or [])

    # Tickets + comments.
    info("fetching tickets")
    tickets = list(client.paginate(f"spaces/{space_id}/tickets", optional=True))
    info(f"{len(tickets)} tickets; fetching comments")
    write_json(sdir / "tickets" / "_index.json", tickets)

    def fetch_comments(t):
        number = t.get("number")
        if number is None:
            return
        comments = list(client.paginate(f"spaces/{space_id}/tickets/{number}/ticket_comments"))
        write_json(sdir / "tickets" / "comments" / f"{number}.json", comments)

    parallel_map(fetch_comments, tickets, workers, label="comments")

    # Milestones.
    write_json(sdir / "milestones.json",
               client.get(f"spaces/{space_id}/milestones/all", allow_404=True) or [])

    # Wiki + versions.
    info("fetching wiki pages")
    wiki_pages = list(client.paginate(f"spaces/{space_id}/wiki_pages", optional=True))
    write_json(sdir / "wiki" / "_index.json", wiki_pages)

    def fetch_wiki_versions(wp):
        wp_id = wp.get("id")
        if wp_id is None:
            return
        versions = list(client.paginate(f"spaces/{space_id}/wiki_pages/{wp_id}/versions"))
        write_json(sdir / "wiki" / "versions" / f"{wp_id}.json", versions)

    parallel_map(fetch_wiki_versions, wiki_pages, workers, label="wiki versions")
    info(f"{len(wiki_pages)} wiki pages")

    # Documents (metadata + bytes).
    info("fetching documents")
    documents = list(client.paginate(f"spaces/{space_id}/documents", optional=True))
    info(f"{len(documents)} documents; downloading files")
    write_json(sdir / "documents" / "_index.json", documents)

    def fetch_file(doc):
        doc_id = doc.get("id")
        if doc_id is None:
            return
        fname = doc.get("filename") or doc.get("name") or "file"
        dest = sdir / "documents" / "files" / capped_leaf(doc_id, fname)
        try:
            client.download(f"spaces/{space_id}/documents/{doc_id}/download", dest)
        except DownloadError as exc:
            if strict_files:
                raise
            with client._fail_lock:
                client.failed_downloads.append({
                    "space": wiki_name, "document_id": doc_id,
                    "filename": fname, "reason": str(exc),
                })

    parallel_map(fetch_file, documents, workers, label="files")

    # Repositories. Git tools are mirror-cloned; SVN tools are dumped in full
    # with svnrdump (a complete, reloadable history dump).
    repos = []
    for tool in tools:
        if is_git_tool(tool):
            repo_path = repo_path_from_url(tool.get("url"))
            if not repo_path:
                raise BackupError(f"GitTool in '{wiki_name}' has no usable clone URL: {tool.get('url')!r}")
            repo_label = safe_name(tool.get("name") or tool.get("menu_name") or repo_path)
            dest = sdir / "repos" / f"{repo_label}.git"
            step(f"  cloning git repo {repo_label}  ({GIT_HOST}/{repo_path})")
            clone_mirror(client.git_url(repo_path), dest)
            repos.append({"kind": "git", "tool": repo_label, "source": repo_path})
        elif is_svn_tool(tool):
            svn_url = tool.get("url")
            if not svn_url:
                raise BackupError(f"SubversionTool in '{wiki_name}' has no url")
            if not username:
                raise BackupError(
                    f"Space '{wiki_name}' has an SVN repository, which needs "
                    f"ASSEMBLA_USERNAME set (see .env.example)."
                )
            if not shutil.which("svnrdump"):
                raise BackupError(
                    f"Space '{wiki_name}' has an SVN repository but 'svnrdump' is "
                    f"not on PATH. Install Subversion (which provides svnrdump)."
                )
            repo_label = safe_name(tool.get("name") or tool.get("menu_name") or "svn")
            dest = sdir / "repos" / f"{repo_label}.svndump"
            step(f"  dumping svn repo {repo_label}  ({svn_url})")
            svn_dump(svn_url, dest, username, client._secret)
            repos.append({"kind": "svn", "tool": repo_label, "source": svn_url})
    if repos:
        info(f"{len(repos)} repositories")

    return {
        "wiki_name": wiki_name,
        "name": space.get("name"),
        "tickets": len(tickets),
        "wiki_pages": len(wiki_pages),
        "documents": len(documents),
        "repositories": len(repos),
    }


def clone_mirror(url_with_creds, dest: Path):
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        raise BackupError(f"Destination already exists: {dest}")
    result = subprocess.run(
        ["git", "clone", "--mirror", url_with_creds, str(dest)],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        # Never surface the credentialed URL in the error.
        raise BackupError(f"git clone --mirror failed for {dest.name}: {result.stderr.strip()[:400]}")


def svn_dump(url, dest: Path, username, password):
    """Full-history dump of a remote SVN repo via svnrdump (all revisions).

    svnrdump reports "* Dumped revision N." on stderr; we stream that to show a
    throttled progress counter, while keeping the last lines for error context.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        raise BackupError(f"Destination already exists: {dest}")
    with open(dest, "wb") as fh:
        proc = subprocess.Popen(
            ["svnrdump", "dump", "--non-interactive", "--no-auth-cache",
             "--username", username, "--password", password, url],
            stdout=fh, stderr=subprocess.PIPE, text=True, encoding="utf-8", errors="replace",
        )
        tail = deque(maxlen=20)
        revs = 0
        for line in proc.stderr:
            line = line.rstrip()
            if not line:
                continue
            tail.append(line)
            if line.startswith("* Dumped revision"):
                revs += 1
                if revs % 100 == 0:
                    info(f"  dumped {revs} revisions")
        proc.wait()
    if proc.returncode != 0:
        raise BackupError(f"svnrdump dump failed for {dest.name}: {' | '.join(tail)[:400]}")
    info(f"  dumped {revs} revisions total")


def make_zip(folder: Path) -> Path:
    zip_path = folder.with_suffix(".zip")
    step(f"Zipping -> {zip_path.name}")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for p in folder.rglob("*"):
            if p.is_file():
                zf.write(p, p.relative_to(folder.parent))
    return zip_path


def parse_args():
    ap = argparse.ArgumentParser(description="Complete offline backup of Assembla space(s).")
    ap.add_argument("--spaces", nargs="+", metavar="NAME",
                    help="Restrict to specific spaces (by wiki_name or id). Default: all accessible.")
    ap.add_argument("--out", default=".", help="Directory to write the backup into (default: current).")
    ap.add_argument("--no-zip", action="store_true", help="Keep the folder, skip creating the zip.")
    ap.add_argument("--list-spaces", action="store_true",
                    help="List every space the API key can access, then exit (no backup).")
    ap.add_argument("--workers", type=int, default=8,
                    help=f"Concurrent requests for comments/files/wiki (default: 8, "
                         f"raise for speed up to the {CONN_POOL}-connection pool).")
    ap.add_argument("--strict-files", action="store_true",
                    help="Treat any file-download failure as fatal (default: record and continue).")
    return ap.parse_args()


def main():
    args = parse_args()
    load_dotenv()
    key = os.getenv("ASSEMBLA_API_KEY")
    secret = os.getenv("ASSEMBLA_API_SECRET")
    if not key or not secret:
        fail("ASSEMBLA_API_KEY and ASSEMBLA_API_SECRET must be set (see .env.example).")
        return 2

    # Optional: only required for spaces that contain an SVN repository.
    username = os.getenv("ASSEMBLA_USERNAME")

    client = AssemblaClient(key, secret)

    if args.list_spaces:
        step("Listing accessible spaces")
        spaces = list(client.paginate("spaces"))
        ok(f"{len(spaces)} space(s) visible to this key:")
        for s in spaces:
            print(f"    {s.get('wiki_name','?'):<28} {s.get('name','')}  "
                  f"[id={s.get('id','?')}]")
        return 0

    # Timestamp passed in from the OS (kept out of core logic for reproducibility).
    stamp = time.strftime("%Y-%m-%dT%H-%M-%SZ", time.gmtime())
    root = Path(args.out).resolve() / f"assembla-backup-{stamp}"
    if root.exists():
        fail(f"Output folder already exists: {root}")
        return 2

    step("Listing accessible spaces")
    all_spaces = list(client.paginate("spaces"))
    write_json(root / "spaces.json", all_spaces)

    if args.spaces:
        wanted = {s.lower() for s in args.spaces}
        spaces = [s for s in all_spaces
                  if s.get("wiki_name", "").lower() in wanted or s.get("id", "").lower() in wanted]
        missing = wanted - {s.get("wiki_name", "").lower() for s in spaces} \
                         - {s.get("id", "").lower() for s in spaces}
        if missing:
            raise BackupError(f"Requested spaces not found / not accessible: {', '.join(sorted(missing))}")
    else:
        spaces = all_spaces
    ok(f"{len(spaces)} space(s) to back up")

    summaries = []
    for space in spaces:
        summaries.append(backup_space(client, space, root, args.workers,
                                      args.strict_files, username))

    failed = client.failed_downloads
    write_json(root / "manifest.json", {
        "generated_utc": stamp,
        "tool": "assembla-backup",
        "space_count": len(summaries),
        "spaces": summaries,
        "failed_download_count": len(failed),
        "failed_downloads": failed,
    })

    if failed:
        print(f"{YELLOW}    {len(failed)} file(s) could not be downloaded "
              f"(Assembla-side errors); listed in manifest.failed_downloads{RESET}")

    if args.no_zip:
        ok(f"Done. Backup folder: {root}")
    elif failed:
        zip_path = make_zip(root)
        print(f"{YELLOW}    Backup complete EXCEPT {len(failed)} unreachable file(s): "
              f"{zip_path}{RESET}")
    else:
        zip_path = make_zip(root)
        ok(f"Done. Verified-complete backup: {zip_path}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except BackupError as exc:
        fail(str(exc))
        fail("Backup halted. No zip produced; partial output (if any) left for inspection.")
        sys.exit(1)
    except KeyboardInterrupt:
        fail("Interrupted.")
        sys.exit(130)
    except Exception as exc:  # noqa: BLE001 - clean exit instead of a raw traceback
        fail(f"Unexpected {exc.__class__.__name__}: {exc}")
        fail("Backup halted. No zip produced; partial output (if any) left for inspection.")
        sys.exit(1)
