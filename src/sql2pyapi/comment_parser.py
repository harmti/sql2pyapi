# ===== SECTION: IMPORTS =====
import logging
import re
import textwrap


# ===== SECTION: REGEX =====
# Regex to find comments (both -- and /* */)
COMMENT_REGEX = re.compile(r"(--.*?$)|(/\*.*?\*/)", re.MULTILINE | re.DOTALL)

# ===== SECTION: FUNCTIONS =====


def clean_comment_block(comment_lines: list[str]) -> str:
    """Cleans a list of raw SQL comment lines for use as a docstring."""
    if not comment_lines:
        return ""

    cleaned_lines = []
    for line in comment_lines:
        stripped_line = line.strip()
        cleaned_line = None

        is_line_comment = stripped_line.startswith("--")
        is_block_start = stripped_line.startswith("/*")
        is_block_end = stripped_line.endswith("*/")
        is_block_single = is_block_start and is_block_end
        is_leading_star = stripped_line.startswith("*") and not is_block_start and not is_block_end

        if is_line_comment:
            cleaned_line = stripped_line[2:]
            if cleaned_line.startswith(" "):
                cleaned_line = cleaned_line[1:]
        elif is_block_single:
            cleaned_line = stripped_line[2:-2].strip() if len(stripped_line) > 4 else ""
        elif is_block_start:
            cleaned_line = stripped_line[2:].lstrip()
        elif is_block_end:
            cleaned_line = stripped_line[:-2].rstrip()
        elif is_leading_star:
            cleaned_line = stripped_line[1:]
            if cleaned_line.startswith(" "):
                cleaned_line = cleaned_line[1:]
        else:
            cleaned_line = stripped_line

        if cleaned_line is not None:
            cleaned_lines.append(cleaned_line)

    valid_lines = cleaned_lines
    if not valid_lines:
        return ""

    raw_comment = "\n".join(valid_lines)
    try:
        dedented_comment = textwrap.dedent(raw_comment).strip()
    except Exception as e:
        logging.warning(f"textwrap.dedent failed during comment cleaning: {e}. Using raw comment.")
        dedented_comment = raw_comment.strip()

    return dedented_comment


def find_preceding_comment(lines: list[str], func_start_line_idx: int) -> str | None:
    """
    Finds the comment block immediately preceding a function definition.
    Searches backwards, handles multi-line blocks, and stops at blank lines or code.
    """
    comment_lines = []
    in_block_comment = False

    for i in range(func_start_line_idx - 1, -1, -1):
        line_content = lines[i]
        stripped_line = line_content.strip()

        if not stripped_line:
            break

        is_block_end = stripped_line.endswith("*/")
        is_block_start = stripped_line.startswith("/*")
        is_line_comment = stripped_line.startswith("--")
        is_comment = is_line_comment or is_block_start or is_block_end or in_block_comment

        if not is_comment and not in_block_comment:
            break

        if is_block_end:
            if in_block_comment:
                comment_lines.clear()
                break
            in_block_comment = True
            if is_block_start and len(stripped_line) > 4:
                comment_lines.insert(0, line_content)
                in_block_comment = False
            else:
                comment_lines.insert(0, line_content)
            continue

        if is_block_start:
            if not in_block_comment:
                break
            comment_lines.insert(0, line_content)
            in_block_comment = False
            continue

        if in_block_comment:
            comment_lines.insert(0, line_content)
            continue

        if is_line_comment:
            comment_lines.insert(0, line_content)
            continue

    if not comment_lines:
        return None

    # Call the cleaning function from this module
    cleaned_comment = clean_comment_block(comment_lines)
    return cleaned_comment
