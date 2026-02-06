from __future__ import annotations

import json


REQUIRED_SECTIONS = [
    "Title",
    "Summary",
    "Severity",
    "Affected Worker",
    "Timeline",
    "Detection",
    "Impact",
    "Suspected Cause",
    "Contributing Factors",
    "Recovery",
    "Remediation",
    "Follow-ups",
    "Trace Links",
    "Raw Appendix",
]


def build_prompt(diagnostic_event: dict[str, object], guidance: dict[str, object] | None = None) -> str:
    payload = diagnostic_event.get("payload", {})
    incident_id = str(diagnostic_event.get("incident_id", "unknown"))
    worker_id = str(diagnostic_event.get("worker_id", "unknown"))
    guidance = guidance or {}
    sections = "\n".join(f"- {section}" for section in REQUIRED_SECTIONS)
    return (
        "Write an incident postmortem for Aegis.\n"
        f"Incident ID: {incident_id}\n"
        f"Worker ID: {worker_id}\n"
        f"Audience: {guidance.get('audience', 'infrastructure engineers')}\n"
        f"Severity context: {guidance.get('severity', payload.get('severity', 'unknown'))}\n"
        f"Extra guidance: {guidance.get('extra_prompt', 'none')}\n"
        "Required Markdown sections:\n"
        f"{sections}\n"
        "Diagnostic JSON:\n"
        f"{json.dumps(payload, sort_keys=True, indent=2)}\n"
        "Do not invent cloud providers, clusters, or teams that are not present in the diagnostics.\n"
    )

