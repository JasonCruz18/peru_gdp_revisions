"""
Text normalization and character cleaning utilities for GDP table processing.

This module provides functions for normalizing text, removing diacritics, handling
special characters, and standardizing text patterns commonly found in BCRP Weekly
Report tables.
"""

import re
import unicodedata
from typing import List


def remove_rare_characters_first_row(text: str) -> str:
    """
    Normalize first-row text: remove spaces around hyphens and keep only alphanumeric + hyphens.

    Intended for first-row cleanups where headers are later derived.

    Args:
        text: Text to clean.

    Returns:
        Cleaned text with only letters, digits, spaces, and hyphens.

    Example:
        >>> remove_rare_characters_first_row("GDP - Total  #value")
        'GDP-Total value'
    """
    text = re.sub(r"\s*-\s*", "-", text)  # Normalize "a - b" -> "a-b"
    text = re.sub(r"[^a-zA-Z0-9\s-]", "", text)  # Keep letters/digits/spaces/hyphens
    return text


def remove_rare_characters(text: str) -> str:
    """
    Remove any character that is not a letter or space.

    Args:
        text: Text to clean.

    Returns:
        Text containing only letters and spaces.

    Example:
        >>> remove_rare_characters("GDP123 @total")
        'GDP total'
    """
    return re.sub(r"[^a-zA-Z\s]", "", text)


def remove_tildes(text: str) -> str:
    """
    Remove diacritics (tildes/accents) from text using Unicode decomposition.

    Args:
        text: Text containing accented characters.

    Returns:
        Text with diacritics removed.

    Example:
        >>> remove_tildes("año económico")
        'ano economico'
    """
    return "".join(
        char for char in unicodedata.normalize("NFD", text) if unicodedata.category(char) != "Mn"
    )


def find_roman_numerals(text: str) -> List[str]:
    """
    Find Roman numerals (I to X) in text.

    Args:
        text: Text to search for Roman numerals.

    Returns:
        List of Roman numerals found.

    Example:
        >>> find_roman_numerals("Section III and IV details")
        ['III', 'IV']
    """
    pattern = r"\b(?:I{1,3}|IV|V|VI{0,3}|IX|X)\b"
    matches = re.findall(pattern, text)
    return matches
