"""飞书简单结果卡片模板"""


def build_analysis_card(
    service: str,
    api_path: str,
    severity: str,
    analysis_summary: str,
    fix_branch: str = "",
    pr_url: str = "",
) -> dict:
    """构建分析结果卡片"""
    elements = [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": (
                    f"**服务:** {service}\n"
                    f"**接口:** {api_path}\n"
                    f"**严重度:** {severity}"
                ),
            },
        },
        {"tag": "hr"},
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"**分析结果:**\n{analysis_summary[:2000]}",
            },
        },
    ]

    if fix_branch or pr_url:
        fix_info = ""
        if fix_branch:
            fix_info += f"**分支:** `{fix_branch}`\n"
        if pr_url:
            fix_info += f"**PR:** [{pr_url}]({pr_url})"
        elements.append({"tag": "hr"})
        elements.append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": fix_info},
        })

    elements.append({
        "tag": "note",
        "elements": [
            {"tag": "plain_text", "content": "在此话题内 @bot 可继续追问"},
        ],
    })

    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "title": {"tag": "plain_text", "content": "分析结果"},
            "template": "orange",
        },
        "elements": elements,
    }
