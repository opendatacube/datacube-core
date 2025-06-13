import os
import re
from docutils.core import publish_doctree
from docutils.utils import SystemMessage

# Define the standard order of heading characters.
# This is a widely adopted convention, but you can customize it.
# The order here implies:
# Level 0: # (with overline/underline)
# Level 1: * (with overline/underline)
# Level 2: = (underline only)
# Level 3: - (underline only)
# Level 4: ^ (underline only)
# Level 5: " (underline only)
STANDARD_HEADING_CHARS = ('#', '*', '=', '-', '^', '"', '+', '`', ':')

# Regex to check if a line is purely a repeated punctuation character
PUNCTUATION_LINE_REGEX = re.compile(r'([!"#$%&\'()*+,-./:;<=>?@\[\\\]^_`{|}~])\1*$')

def is_potential_underline(line, prev_line_content):
    """
    Checks if a line is a potential reStructuredText heading underline.
    It must be a repeated punctuation character and have the same length
    as the previous non-blank line.
    """
    if not line.strip():
        return None  # Not an underline
    match = PUNCTUATION_LINE_REGEX.fullmatch(line.strip())
    if match and prev_line_content and len(line.strip()) == len(prev_line_content.strip()):
        return match.group(1) # Return the character used
    return None

def check_heading_order(filepath, standard_chars):
    """
    Parses an RST file line by line to check heading character consistency
    against a defined standard order.
    """
    print(f"\n--- Checking: {filepath} ---")
    inconsistencies_found = 0
    prev_line_content = None

    # Store characters seen in this file and their assigned relative level
    # e.g., {'#': 0, '=': 1, '-': 2}
    char_to_relative_level_map = {}

    # Store the actual characters encountered in order in this file for level assignment
    # e.g., ['#', '=', '-'] means # is level 0, = is level 1, - is level 2
    actual_char_order_in_file = []

    try:
        # --- Read the entire file content first ---
        with open(filepath, 'r', encoding='utf-8') as f:
            full_content = f.read()

        # Now, split the full content into lines for your heuristic scan
        lines = full_content.splitlines(keepends=True) # Keep newlines for line length accuracy

        for i, line in enumerate(lines):
            # Check for overline/underline patterns (e.g., ###\nTitle\n###)
            if i > 0 and i < len(lines) - 1:
                # Use rstrip() for length comparison to exclude trailing newlines
                overline_char = is_potential_underline(lines[i-1], lines[i].rstrip())
                underline_char = is_potential_underline(lines[i+1], lines[i].rstrip())

                # If both overline and underline are present and match
                if overline_char and underline_char and overline_char == underline_char:
                    heading_char = overline_char

                    # Ensure the previous line (overline) isn't just an underscore
                    # that could be confused with bold/italic markup.
                    if lines[i-1].strip() == lines[i].strip():
                         # This might be an inline markup. Skip.
                        prev_line_content = line
                        continue

                    # This is a potential overline/underline heading

                    # Logic for processing heading characters
                    if heading_char not in standard_chars:
                        print(f"  [WARNING] Line {i+1}: Heading character '{heading_char}' is not in the standard list.")
                        inconsistencies_found += 1

                    if heading_char not in char_to_relative_level_map:
                        # This is a new heading character introduced in this file
                        char_to_relative_level_map[heading_char] = len(actual_char_order_in_file)
                        actual_char_order_in_file.append(heading_char)

                        # Check if the introduction order matches the global standard
                        expected_char_at_this_level = standard_chars[len(actual_char_order_in_file) - 1] if len(actual_char_order_in_file) - 1 < len(standard_chars) else None
                        if expected_char_at_this_level and heading_char != expected_char_at_this_level:
                            print(f"  [ERROR] Line {i+1}: New heading character '{heading_char}' introduced, but expected '{expected_char_at_this_level}' according to standard order for this level.")
                            print(f"           Current file's char order: {actual_char_order_in_file}")
                            inconsistencies_found += 1
                    else:
                        # This character has been used before in this file.
                        # Ensure it's used for the same relative level.
                        # (This is implicitly handled by `char_to_relative_level_map` and `actual_char_order_in_file`
                        # if new characters are only added once they match their expected level).
                        pass # No additional check needed here for reuse within file, as it should be consistent.

                    prev_line_content = lines[i].rstrip() # Treat the title line as the "previous content" for the next check
                    continue # Skip to next line, as we've processed the heading structure

            # Check for underline only patterns (e.g., Title\n====)
            heading_char = is_potential_underline(line, prev_line_content)
            if heading_char:
                # This is a potential underline-only heading

                if heading_char not in standard_chars:
                    print(f"  [WARNING] Line {i+1}: Heading character '{heading_char}' is not in the standard list.")
                    inconsistencies_found += 1

                if heading_char not in char_to_relative_level_map:
                    # This is a new heading character introduced in this file
                    char_to_relative_level_map[heading_char] = len(actual_char_order_in_file)
                    actual_char_order_in_file.append(heading_char)

                    # Check if the introduction order matches the global standard
                    expected_char_at_this_level = None
                    if len(actual_char_order_in_file) - 1 < len(standard_chars):
                        expected_char_at_this_level = standard_chars[len(actual_char_order_in_file) - 1]

                    if expected_char_at_this_level and heading_char != expected_char_at_this_level:
                        print(f"  [ERROR] Line {i+1}: New heading character '{heading_char}' introduced, but expected '{expected_char_at_this_level}' according to standard order for this level.")
                        print(f"           Current file's char order: {actual_char_order_in_file}")
                        inconsistencies_found += 1
                else:
                    # This character has been used before in this file.
                    # Ensure it's used for the same relative level.
                    pass # Implicitly consistent if new characters are only added when they match expected level.

                prev_line_content = None # Reset after a heading to look for the next title
                continue # Skip to next line, as we've processed the heading structure

            # If line is not a heading, update prev_line_content for next iteration
            if line.strip(): # Only consider non-blank lines as potential title lines
                prev_line_content = line.rstrip() # Use rstrip to remove newline for length comparison
            else:
                prev_line_content = None # Reset if a blank line breaks the title-underline sequence

        # --- Docutils semantic validation ---
        # Now use the 'full_content' string that was read at the beginning
        try:
            publish_doctree(source=full_content, settings_overrides={'report_level': 5}) # Report all messages
        except SystemMessage as e:
            print(f"  [DOCUTILS ERROR] File could not be parsed correctly by docutils: {e}")
            inconsistencies_found += 1
        except Exception as e:
            print(f"  [DOCUTILS UNEXPECTED ERROR] During docutils parsing: {e}")
            inconsistencies_found += 1

    except FileNotFoundError:
        print(f"  Error: File not found: {filepath}")
        return 1
    except Exception as e:
        print(f"  An unexpected error occurred while processing {filepath}: {e}")
        return 1

    if inconsistencies_found == 0:
        print(f"  No heading order inconsistencies found in {filepath}.")
    else:
        print(f"  Total inconsistencies found in {filepath}: {inconsistencies_found}")
    return inconsistencies_found

# The main() function remains the same.
def main():
    import argparse
    parser = argparse.ArgumentParser(description="Check reStructuredText heading character order consistency.")
    parser.add_argument("path", help="Path to the .rst file or directory containing .rst files.")
    args = parser.parse_args()

    total_inconsistencies = 0
    if os.path.isfile(args.path) and args.path.endswith('.rst'):
        total_inconsistencies += check_heading_order(args.path, STANDARD_HEADING_CHARS)
    elif os.path.isdir(args.path):
        for root, _, files in os.walk(args.path):
            for file in files:
                if file.endswith('.rst'):
                    filepath = os.path.join(root, file)
                    total_inconsistencies += check_heading_order(filepath, STANDARD_HEADING_CHARS)
    else:
        print("Please provide a valid .rst file or a directory containing .rst files.")
        return

    print(f"\n--- Scan Complete ---")
    if total_inconsistencies == 0:
        print("All reStructuredText files adhere to the heading character order standard.")
    else:
        print(f"Total inconsistencies found across all files: {total_inconsistencies}")
        print("Please review the errors and warnings above.")

if __name__ == "__main__":
    main()