from sys import stderr, stdin

def construct_layout(cols):
    layout_items = []
    current_position = 0

    # Sort cols by start property
    cols.sort(key=lambda col: col.get('start'))

    for col in cols:
        # If any chars were skipped, add a pad byte
        if col['start'] != current_position:
            skip_chars = col['start'] - current_position
            layout_items.append('{0}x'.format(skip_chars))

        size = col['end'] - col['start']
        format_char = col.get('format', 's')

        # If skip property is True, use the pad format character
        if col.get('skip') == True: format_char = 'x'

        layout_items.append('{0}{1}'.format(size, format_char))

        current_position = col['end']

    return ' '.join(layout_items)

# Gets header names, excluding those being skipped
def get_active_header(cols):
    return [col['name'] for col in cols if col.get('skip') != True]
