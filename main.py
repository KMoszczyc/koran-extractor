import pandas as pd
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('max_colwidth', 1000)

INPUT_PATH = 'data/koran.txt'
OUTPUT_PATH = 'data/koran.parquet'

columns = ['chapter_nr', 'chapter_name', 'verse', 'text']

def read_file(path):
    with open(path, 'r') as f:
        return f.read().splitlines()

def process_chapter_line(line):
    s = line.replace('Sura ', '')
    s_split = s.split('.')
    chapter_nr = int(s_split[0])
    chapter_name = '.'.join(s_split[1:]).strip()

    return chapter_nr, chapter_name

def process_verse(line):
    s_split = line.split('. ')
    verse = s_split[0]
    text = '. '.join(s_split[1:]).strip()

    return verse, text

def process_file(lines):
    data = []
    chapter_nr = None
    chapter_name = None
    verse_start = None
    verse_end = None
    last_text = ''
    skip_next_append = False
    for i, line in enumerate(lines):
        if line.startswith('Sura'):
            data, verse_end, skip_next_append = append_verse(data, chapter_nr, chapter_name, verse_start, verse_end, last_text, skip_next_append)
            skip_next_append = True
            chapter_nr, chapter_name = process_chapter_line(line)
        elif is_new_verse(line):
            data, verse_end, skip_next_append = append_verse(data, chapter_nr, chapter_name, verse_start, verse_end, last_text, skip_next_append)
            verse_start, last_text = process_verse(line)
        elif is_verse_continuation(line):
            verse_end, text = process_verse(line)
            last_text += text
        else:
            print(chapter_nr,chapter_name, line)

    # add last verse of the last chapter
    data, verse_end, skip_next_append = append_verse(data, chapter_nr, chapter_name, verse_start, verse_end, last_text, skip_next_append)

    df = pd.DataFrame(data, columns=columns)
    return df

def append_verse(data, chapter_nr, chapter_name, verse_start, verse_end, last_text, skip_next_append):
    if skip_next_append:
        return data, verse_end, False
    if last_text != '' and verse_end is not None:
        verse = f"{verse_start}-{verse_end}"
        data.append((chapter_nr, chapter_name, verse, last_text))
    elif last_text != '':
        data.append((chapter_nr, chapter_name, verse_start, last_text))
    verse_end = None
    return data, verse_end, skip_next_append

def is_verse(line):
    return line[0].isdigit()

def is_verse_continuation(line):
    return line[0].isdigit() and line.split('. ')[1][0].islower()

def is_new_verse(line):
    return line[0].isdigit() and not line.split('. ')[1][0].islower()

if __name__ == '__main__':
    lines = read_file(INPUT_PATH)
    df = process_file(lines)
    df.to_parquet(OUTPUT_PATH)


    print(df.head(50))
    print(df.tail(50))

    print(df.describe())