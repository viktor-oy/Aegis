# Incident Postmortem Guidelines
Generate the postmortem using extremely clean and highly structured Markdown.

## Formatting Rules
- **Title**: The document MUST start exactly with a single H1 heading describing the incident (e.g. `# Incident Postmortem: Aegis System`). Do NOT name the section "Title".
- **Key Details**: Create a `## Key Details` section containing ALL properties provided in the Context Data section as bullet points (including Failure type and Trigger reason). DO NOT omit any property. DO NOT output a `### Context Data` heading or echo the raw input block; just format the properties cleanly.
- **Summary**: Create a `## Summary` section with a concise 1-2 sentence overview of what happened. Do not put IDs here; keep them in Key Details.
- **Bullet Points**: Use bullet points for all other sections (`Detection`, `Impact`, `Suspected Cause`, `Recovery Steps`). Avoid long paragraphs.
- **No Redundancy**: Do not repeat the same text or IDs in multiple sections. Stop generating immediately when finished.
- **Rich Elements**: Highlight key terms with `**bolding**` and use ```code blocks``` for raw data.
