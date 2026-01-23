from __future__ import annotations

import json


REQUIRED_SECTIONS = [
    "Title",
    "Key Details",
    "Summary",
    "Detection",
    "Impact",
    "Suspected Cause",
    "Recovery Steps",
]


def build_prompt(
    diagnostic_event: dict[str, object], 
    sys_arch: str = "",
    format_prompt: str = "",
    guidance: dict[str, object] | None = None
) -> str:
    payload = diagnostic_event.get("payload", {})
    incident_id = str(diagnostic_event.get("incident_id", "unknown"))
    worker_id = str(diagnostic_event.get("worker_id", "unknown"))
    event_id = str(diagnostic_event.get("event_id", "unknown"))
    guidance = guidance or {}
    sections = "\n".join(f"- {section}" for section in REQUIRED_SECTIONS)
    return (
        "Write an incident postmortem for Aegis.\n"
        "This is an initial, pre-fix postmortem. The issue has just emerged and is NOT yet resolved. Focus on immediate diagnosis.\n\n"
        "### Context Data\n"
        f"Event ID: {event_id}\n"
        f"Incident ID: {incident_id}\n"
        f"Worker ID: {worker_id}\n"
        f"Audience: {guidance.get('audience', 'infrastructure engineers')}\n"
        f"Severity context: {guidance.get('severity', payload.get('severity', 'unknown'))}\n"
        f"Extra guidance: {guidance.get('extra_prompt', 'none')}\n\n"
        "### Required Markdown Sections (in order):\n"
        f"{sections}\n\n"
        "### System Architecture Context:\n"
        f"{sys_arch}\n\n"
        "### Formatting Guidelines:\n"
        f"{format_prompt}\n\n"
        "### Diagnostic JSON:\n"
        f"{json.dumps(payload, sort_keys=True, indent=2)}\n\n"
        "Do not invent cloud providers, clusters, or teams that are not present in the diagnostics. Stop generating once the last section is completed.\n"
    )

