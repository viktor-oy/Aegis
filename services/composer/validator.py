from __future__ import annotations

from dataclasses import dataclass

from .prompt import REQUIRED_SECTIONS


@dataclass(frozen=True)
class ValidationResult:
    ok: bool
    errors: list[str]


def validate_postmortem(markdown: str, incident_id: str, worker_id: str) -> ValidationResult:
    errors: list[str] = []
    if len(markdown.strip()) < 120:
        errors.append("postmortem is too short")
    for section in REQUIRED_SECTIONS:
        if section == "Title":
            if not markdown.lstrip().startswith("# "):
                errors.append("missing required section: Title")
        elif f"## {section}" not in markdown and f"# {section}" not in markdown:
            errors.append(f"missing required section: {section}")
    if incident_id not in markdown:
        errors.append("incident_id is not preserved")
    if worker_id not in markdown:
        errors.append("worker_id is not preserved")
    hallucinated_names = ["prod-eu-magic", "unknown-supercluster", "payments-team"]
    for name in hallucinated_names:
        if name in markdown:
            errors.append(f"contains likely hallucinated infrastructure name: {name}")
    return ValidationResult(ok=not errors, errors=errors)

