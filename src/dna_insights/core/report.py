from __future__ import annotations

from html import escape


def _render_insight_card(result: dict) -> str:
    evidence = result.get("evidence_level", {})
    genotypes = result.get("genotypes", {})
    genotype_lines = "".join(
        f"<li><strong>{escape(rsid)}</strong>: {escape(str(genotype))}</li>"
        for rsid, genotype in genotypes.items()
    )
    references = "".join(f"<li>{escape(ref)}</li>" for ref in result.get("references", []))

    return f"""
      <div class="card">
        <h3>{escape(result.get('display_name', ''))}</h3>
        <p class="summary">{escape(result.get('summary', ''))}</p>
        <p><strong>Evidence:</strong> {escape(evidence.get('grade', ''))} - {escape(evidence.get('summary', ''))}</p>
        <p><strong>Limitations:</strong> {escape(result.get('limitations', ''))}</p>
        <details>
          <summary>Why?</summary>
          <ul>{genotype_lines}</ul>
        </details>
        {f"<details><summary>References</summary><ul>{references}</ul></details>" if references else ""}
      </div>
    """


def build_html_report(
    profile: dict,
    import_info: dict,
    insights: list[dict],
    kb_version: str,
) -> str:
    categories = {}
    for result in insights:
        categories.setdefault(result.get("category", "other"), []).append(result)

    category_sections = ""
    for category, items in categories.items():
        cards = "".join(_render_insight_card(item) for item in items)
        category_sections += f"""
        <section>
          <h2>{escape(category.title())}</h2>
          {cards}
        </section>
        """

    return f"""
    <!doctype html>
    <html>
    <head>
      <meta charset="utf-8" />
      <title>DNA Insights Report</title>
      <style>
        body {{ font-family: Arial, sans-serif; margin: 24px; line-height: 1.5; }}
        .banner {{ background: #ffe9d6; padding: 12px; border-radius: 8px; margin-bottom: 16px; }}
        .meta {{ font-size: 0.9em; color: #444; }}
        .card {{ border: 1px solid #ddd; padding: 12px; border-radius: 8px; margin-bottom: 12px; }}
        .summary {{ font-size: 1.05em; }}
        h1, h2 {{ margin-top: 24px; }}
      </style>
    </head>
    <body>
      <div class="banner">
        <strong>Educational use only.</strong> Not medical advice. Confirm any health-related findings in a clinical lab.
      </div>
      <h1>DNA Insights Report</h1>
      <p><strong>Profile:</strong> {escape(profile.get('display_name', ''))}</p>
      <p class="meta">
        <strong>Imported:</strong> {escape(import_info.get('imported_at', ''))} | 
        <strong>File hash:</strong> {escape(import_info.get('file_hash_sha256', ''))} | 
        <strong>Parser:</strong> {escape(import_info.get('parser_version', ''))} | 
        <strong>Build:</strong> {escape(import_info.get('build', ''))} | 
        <strong>Strand:</strong> {escape(import_info.get('strand', ''))} | 
        <strong>KB version:</strong> {escape(kb_version)}
      </p>
      {category_sections}
    </body>
    </html>
    """
