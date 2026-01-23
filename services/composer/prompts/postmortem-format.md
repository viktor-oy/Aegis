# Incident Postmortem Guidelines
Generate the postmortem using extremely clean and highly structured Markdown.

## Formatting Rules
- **Title**: The document must start with a single H1 heading (`# [Issue Summary]`). Do NOT name the section "Title".
- **Key Details**: Create a `## Key Details` section containing the Event ID, Incident ID, and Worker ID as bullet points.
- **Bullet Points**: Use bullet points for all other sections (`Detection`, `Impact`, `Suspected Cause`, `Recovery Steps`). Avoid long paragraphs.
- **No Redundancy**: Do not repeat the same text in multiple sections. Stop generating immediately when finished.
- **Rich Elements**: Highlight key terms with `**bolding**` and use ```code blocks``` for raw data.
