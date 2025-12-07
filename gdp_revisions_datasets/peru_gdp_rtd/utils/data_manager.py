"""
Data management utilities for record-keeping and file tracking.

This module provides a unified RecordManager class for handling text-based record files
used throughout the pipeline to track downloaded PDFs, processed files, and other
idempotent operations.
"""

import os
import re
from pathlib import Path
from typing import Callable, Optional, Set, Union


class RecordManager:
    """
    Manager for text-based record files used to track processed items.

    Provides a unified interface for reading, writing, and managing record files
    that store one filename per line. Supports custom sorting and ensures
    idempotent pipeline operations.

    Attributes:
        record_folder: Path to folder containing the record file.
        record_filename: Name of the record file.
        items: Set of items currently in the record.

    Example:
        >>> manager = RecordManager("./record", "downloads.txt")
        >>> manager.load()
        >>> if "ns-01-2023.pdf" not in manager:
        ...     print("Not yet downloaded")
        >>> manager.add("ns-01-2023.pdf")
        >>> manager.save()
    """

    def __init__(
        self,
        record_folder: Union[str, Path],
        record_filename: str,
        auto_create: bool = True,
    ):
        """
        Initialize RecordManager.

        Args:
            record_folder: Path to folder containing the record file.
            record_filename: Name of the record file (e.g., "downloads.txt").
            auto_create: If True, creates folder and empty file if they don't exist.
        """
        self.record_folder = Path(record_folder)
        self.record_filename = record_filename
        self.record_path = self.record_folder / record_filename
        self.items: Set[str] = set()

        if auto_create:
            self.record_folder.mkdir(parents=True, exist_ok=True)
            if not self.record_path.exists():
                self.record_path.touch()

    @property
    def path(self) -> Path:
        """Get full path to the record file."""
        return self.record_path

    def load(self) -> Set[str]:
        """
        Load items from record file into memory.

        Returns:
            Set of items read from the record file.

        Note:
            Empty lines and whitespace are automatically stripped.
        """
        if not self.record_path.exists():
            self.items = set()
            return self.items

        with open(self.record_path, "r", encoding="utf-8") as f:
            self.items = set(line.strip() for line in f if line.strip())

        return self.items

    def save(self, sort_key: Optional[Callable[[str], tuple]] = None) -> None:
        """
        Save current items to record file.

        Args:
            sort_key: Optional function to sort items before writing.
                     If None, items are sorted alphabetically.

        Example:
            >>> def chronological_key(s):
            ...     # Sort by year, then issue number
            ...     match = re.search(r'ns-(\\d{2})-(\\d{4})', s)
            ...     if match:
            ...         return (int(match.group(2)), int(match.group(1)))
            ...     return (9999, 9999)
            >>> manager.save(sort_key=chronological_key)
        """
        self.record_folder.mkdir(parents=True, exist_ok=True)

        sorted_items = sorted(self.items, key=sort_key) if sort_key else sorted(self.items)

        with open(self.record_path, "w", encoding="utf-8") as f:
            for item in sorted_items:
                f.write(item + "\n")

    def add(self, item: str) -> bool:
        """
        Add item to the record.

        Args:
            item: Item to add to the record.

        Returns:
            True if item was added (wasn't already present), False otherwise.
        """
        if item in self.items:
            return False
        self.items.add(item)
        return True

    def add_many(self, items: Set[str]) -> int:
        """
        Add multiple items to the record.

        Args:
            items: Set of items to add.

        Returns:
            Number of new items added (excludes duplicates).
        """
        initial_count = len(self.items)
        self.items.update(items)
        return len(self.items) - initial_count

    def remove(self, item: str) -> bool:
        """
        Remove item from the record.

        Args:
            item: Item to remove from the record.

        Returns:
            True if item was removed (was present), False otherwise.
        """
        if item not in self.items:
            return False
        self.items.discard(item)
        return True

    def remove_many(self, items: Set[str]) -> int:
        """
        Remove multiple items from the record.

        Args:
            items: Set of items to remove.

        Returns:
            Number of items actually removed.
        """
        initial_count = len(self.items)
        self.items.difference_update(items)
        return initial_count - len(self.items)

    def contains(self, item: str) -> bool:
        """
        Check if item exists in the record.

        Args:
            item: Item to check.

        Returns:
            True if item is in the record, False otherwise.
        """
        return item in self.items

    def __contains__(self, item: str) -> bool:
        """Support 'in' operator for checking membership."""
        return self.contains(item)

    def __len__(self) -> int:
        """Return number of items in the record."""
        return len(self.items)

    def clear(self) -> None:
        """Remove all items from the record (in memory only, call save() to persist)."""
        self.items.clear()

    def get_all(self) -> Set[str]:
        """
        Get all items in the record.

        Returns:
            Copy of the items set.
        """
        return self.items.copy()


def chronological_pdf_key(filename: str) -> tuple:
    """
    Sort key function for BCRP Weekly Report PDFs in chronological order.

    Extracts year and issue number from 'ns-XX-YYYY.pdf' format filenames.
    Files that don't match the pattern are sorted last alphabetically.

    Args:
        filename: PDF filename to extract sort key from.

    Returns:
        Tuple of (year, issue, basename) for sorting.

    Example:
        >>> chronological_pdf_key("ns-01-2023.pdf")
        (2023, 1, 'ns-01-2023')
        >>> chronological_pdf_key("unknown.pdf")
        (9999, 9999, 'unknown')
    """
    base = os.path.splitext(os.path.basename(filename))[0]
    match = re.search(r"ns-(\d{2})-(\d{4})", base, re.I)
    if not match:
        return (9999, 9999, base)
    issue = int(match.group(1))
    year = int(match.group(2))
    return (year, issue, base)
