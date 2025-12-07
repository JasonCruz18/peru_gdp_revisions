"""PDF file organization utilities for Peru GDP RTD pipeline.

This module provides functions to organize and manage downloaded PDF files:
- Organize PDFs into year-based folders
- Replace defective PDFs with corrected versions
"""

import os
import re
import shutil
from typing import List, Tuple, Optional

import requests


def organize_files_by_year(raw_pdf_folder: str) -> None:
    """Move PDFs in raw_pdf_folder into subfolders named by year.

    The year is inferred from the first 4-digit token in the filename.
    For example, 'ns-07-2019.pdf' will be moved to '2019/ns-07-2019.pdf'.

    Args:
        raw_pdf_folder: Directory containing the downloaded PDFs

    Example:
        >>> organize_files_by_year("new_weekly_reports/raw")
        # PDFs are now organized like:
        # new_weekly_reports/raw/2019/ns-07-2019.pdf
        # new_weekly_reports/raw/2020/ns-01-2020.pdf
    """
    files = os.listdir(raw_pdf_folder)

    for file in files:
        # Skip if it's already a directory
        file_path = os.path.join(raw_pdf_folder, file)
        if os.path.isdir(file_path):
            continue

        name, _ext = os.path.splitext(file)
        year = None

        # Heuristic: look for any 4-digit token
        for part in name.split("-"):
            if part.isdigit() and len(part) == 4:
                year = part
                break

        if year:
            dest = os.path.join(raw_pdf_folder, year)
            os.makedirs(dest, exist_ok=True)
            shutil.move(os.path.join(raw_pdf_folder, file), dest)
        else:
            print(f"⚠️  No 4-digit year detected in filename: {file}")


def replace_defective_pdfs(
    items: List[Tuple[str, str, str]],
    root_folder: str,
    record_folder: str,
    download_record_txt: str,
    quarantine: Optional[str] = None,
    verbose: bool = True,
) -> Tuple[int, int]:
    """Replace defective WR PDFs stored under year subfolders.

    Keeps the download record consistent so the downloader will not re-fetch
    defective files.

    Args:
        items: List of triples (year, defective_pdf, replacement_code).
               Example: [("2017", "ns-08-2017.pdf", "ns-07-2017"),
                        ("2019", "ns-23-2019.pdf", "ns-22-2019")]
        root_folder: Base path containing year folders (e.g., 'raw_pdf')
        record_folder: Folder holding the download record TXT
        download_record_txt: Record filename (e.g., 'downloaded_pdfs.txt')
        quarantine: If set, move defective PDFs there; if None, delete them
        verbose: If True, prints a clear summary at the end

    Returns:
        Tuple of (ok, fail) where:
        - ok: number of PDFs successfully replaced
        - fail: total failures (not_found + download_errors + file_op_errors)

    Notes:
        - Defective entries are intentionally NOT removed from the record file
          to prevent re-downloads
        - Replacement filenames ARE appended to the record file in chronological order

    Example:
        >>> items = [
        ...     ("2017", "ns-08-2017.pdf", "ns-07-2017"),
        ...     ("2019", "ns-23-2019.pdf", "ns-22-2019"),
        ... ]
        >>> ok, fail = replace_defective_pdfs(
        ...     items=items,
        ...     root_folder="new_weekly_reports/input",
        ...     record_folder="record",
        ...     download_record_txt="1_downloaded_pdfs.txt",
        ...     quarantine="new_weekly_reports/input/_quarantine"
        ... )
        >>> print(f"Replaced: {ok}, Failed: {fail}")
    """
    # Accept 'ns-7-2019' or 'ns-07-2019[.pdf]'
    pat = re.compile(r"^ns-(\d{1,2})-(\d{4})(?:\.pdf)?$", re.I)

    def norm(c: str) -> str:
        """Normalize NS code to standard format."""
        m = pat.match(os.path.basename(c).lower())
        if not m:
            raise ValueError(f"Bad NS code: {c}")
        # Zero-pad issue (e.g., 7 -> 07)
        return f"ns-{int(m.group(1)):02d}-{m.group(2)}"

    def url(c: str) -> str:
        """Build BCRP URL for replacement PDF."""
        cc = norm(c)
        # Year-coded path
        return f"https://www.bcrp.gob.pe/docs/Publicaciones/Nota-Semanal/{cc[-4:]}/{cc}.pdf"

    def _ns_key(name: str) -> Tuple[int, int, str]:
        """Sort key for Weekly Report filenames."""
        base = os.path.splitext(os.path.basename(name))[0]
        m = re.search(r"ns-(\d{2})-(\d{4})", base, re.I)
        if not m:
            return (9999, 9999, base)  # Unknowns last, stable by base
        issue, year = int(m.group(1)), int(m.group(2))
        return (year, issue, base)

    def update_record(add: Optional[str] = None, remove: Optional[str] = None) -> None:
        """Update the download record file."""
        p = os.path.join(record_folder, download_record_txt)
        s: set = set()

        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                s = {x.strip() for x in f if x.strip()}

        if add:
            s.add(add)

        records = sorted(s, key=_ns_key)

        os.makedirs(record_folder, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            f.write("\n".join(records) + ("\n" if records else ""))

    if quarantine:
        os.makedirs(quarantine, exist_ok=True)

    ok = 0
    not_found = 0
    download_errors = 0
    file_op_errors = 0

    replaced_names: List[str] = []
    failed_items: List[Tuple[str, str, str, str]] = []  # (year, bad_pdf, repl_code, reason)

    for year, bad_pdf, repl_code in items:
        year = str(year)
        ydir = os.path.join(root_folder, year)
        bad_path = os.path.join(ydir, bad_pdf)
        new_name = f"{norm(repl_code)}.pdf"
        new_path = os.path.join(ydir, new_name)

        if not os.path.exists(bad_path):
            not_found += 1
            failed_items.append((year, bad_pdf, repl_code, "not found"))
            continue

        # Download replacement first
        try:
            os.makedirs(ydir, exist_ok=True)
            with requests.get(url(repl_code), stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(new_path, "wb") as fh:
                    for ch in r.iter_content(131072):  # 128 KiB chunks
                        if ch:
                            fh.write(ch)
        except Exception as e:
            try:
                if os.path.exists(new_path):
                    os.remove(new_path)
            except Exception:
                pass
            download_errors += 1
            failed_items.append((year, bad_pdf, repl_code, f"download: {e.__class__.__name__}"))
            continue

        # Quarantine or delete the defective file
        try:
            if quarantine:
                shutil.move(bad_path, os.path.join(quarantine, bad_pdf))
            else:
                os.remove(bad_path)
        except Exception as e:
            file_op_errors += 1
            failed_items.append((year, bad_pdf, repl_code, f"file-op: {e.__class__.__name__}"))
            try:
                if os.path.exists(new_path):
                    os.remove(new_path)  # Roll back
            except Exception:
                pass
            continue

        update_record(add=new_name, remove=bad_pdf)
        replaced_names.append(new_name)
        ok += 1

    fail = not_found + download_errors + file_op_errors

    if verbose:
        print("\n📊 PDF replacement summary")
        print(f"   • Succeeded: {ok}")
        print(
            f"   • Failed:    {fail} "
            f"(not found: {not_found}, download errors: {download_errors}, "
            f"file ops: {file_op_errors})"
        )
        if replaced_names:
            preview = ", ".join(replaced_names[:10])
            suffix = "…" if len(replaced_names) > 10 else ""
            print(f"   • New files: {preview}{suffix}")
        if failed_items:
            print("   • Failed items (sample):")
            for y, bad, rep, reason in failed_items[:5]:
                print(f"     - {bad} [{y}] ← {rep}  ({reason})")
            if len(failed_items) > 5:
                print(f"     … and {len(failed_items) - 5} more")

    return ok, fail
