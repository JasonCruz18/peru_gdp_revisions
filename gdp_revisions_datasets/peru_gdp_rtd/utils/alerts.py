"""Audio alert utilities for Peru GDP RTD pipeline.

This module provides functions for playing audio alerts during long-running operations,
such as batch PDF downloads. Audio feedback helps users monitor progress without
constantly watching the terminal.
"""

import os
import random
from typing import Optional

import pygame


def load_alert_track(alert_track_folder: str, last_alert: Optional[str] = None) -> Optional[str]:
    """Load a random .mp3 file from alert_track_folder for audio alerts.

    Avoids immediate repetition of the previous selection when possible.

    Args:
        alert_track_folder: Directory expected to contain one or more .mp3 files
        last_alert: The filename of the last track played (to avoid repetition)

    Returns:
        The filename of the selected .mp3 file, or None if no .mp3 is found

    Example:
        >>> track = load_alert_track("alert_track/")
        >>> if track:
        ...     print(f"Loaded: {track}")
    """
    os.makedirs(alert_track_folder, exist_ok=True)

    # Collect only .mp3 filenames (case-insensitive)
    tracks = [f for f in os.listdir(alert_track_folder) if f.lower().endswith(".mp3")]

    if not tracks:
        print("🔇 No .mp3 files found in 'alert_track/'. Continuing without audio alerts.")
        return None

    # Prefer any file ≠ last; fallback to all if single
    choices = [t for t in tracks if t != last_alert] or tracks

    # Uniform random selection among candidates
    track = random.choice(choices)

    # Build absolute path to the chosen file
    alert_track_path = os.path.join(alert_track_folder, track)

    # Preload into pygame mixer for instant playback
    pygame.mixer.music.load(alert_track_path)

    return track


def play_alert_track() -> None:
    """Start playback of the currently loaded alert track.

    The track must be loaded first using load_alert_track().
    Playback is non-blocking.

    Example:
        >>> load_alert_track("alert_track/")
        >>> play_alert_track()
    """
    pygame.mixer.music.play()


def stop_alert_track() -> None:
    """Stop playback of the current alert track immediately.

    Example:
        >>> play_alert_track()
        >>> # ... do some work ...
        >>> stop_alert_track()
    """
    pygame.mixer.music.stop()


def init_audio() -> None:
    """Initialize the pygame audio mixer.

    Should be called once before using any alert functions.

    Example:
        >>> init_audio()
        >>> load_alert_track("alert_track/")
        >>> play_alert_track()
    """
    pygame.mixer.init()
