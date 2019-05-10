import sys
import os
import random

argc = len(sys.argv)
if argc < 2 or argc > 5:
    print('Usage: {} csv_file [start_note_count] [note_count] [note_outdir | note_outfile]'.format(__file__))
    print('If \'start_note_count\' is negative, note indices are selected randomly.')
    print('If \'note_outdir\' does not exists, it is considered as \'note_outfile\'.')
    exit()

note_infile = sys.argv[1]
start_note_count = 0
note_count = 1
note_outdir = '.'
note_outfile = ''
if argc > 2:
    start_note_count = int(sys.argv[2])
if argc > 3:
    note_count = int(sys.argv[3])
if argc > 4:
    if os.path.isdir(sys.argv[4]):
        note_outdir = sys.argv[4]
    else:
        note_outfile = sys.argv[4]
        note_outdir = ''


# Simple error checks
if not os.path.isfile(note_infile):
    print('ERROR! infile does not exists.')
    exit()
if note_count <= 0:
    print('ERROR! note count has to be a positive integer.')
    exit()


print('Processing file: {}'.format(note_infile))
if note_outdir:
    print('Output directory: {}'.format(note_outdir))
else:
    print('Output file: {}'.format(note_outfile))
if start_note_count < 0:
    selected_note_idxs = random.sample(range(0, 2082294), note_count)
    print('Note random: {}'.format(selected_note_idxs))
else:
    selected_note_idxs = range(start_note_count, start_note_count + note_count)
    print('Note range: {} - {}'.format(start_note_count, start_note_count + note_count))


with open(note_infile) as ifd:
    notes_processed = 0
    note_idx = 0
    while notes_processed < note_count:
        note = ''
        found_note_end = False
        while (1):
            file_pos = ifd.tell()
            line_str = ifd.readline().lstrip(' ')
            meta = line_str.split(',\"', 1)

            if len(meta) == 2 and meta[0].isdigit():  # reached beginning of a note
                if note:  # reached next note, rewind file pointer
                    ifd.seek(file_pos)
                    if not found_note_end:
                        str_idx = note.rfind('\"')
                        note = note[:str_idx] + '\n'
                    break
                note_idx = int(meta[0])  # get note index
                line_str = meta[1]  # get note first line
            elif meta[0] == '\"\n':  # ignore end of note lines
                found_note_end = True
                continue

            note += line_str

        if note_idx in selected_note_idxs:
            if note_outdir:
                note_outfile = os.path.join(note_outdir, 'notes_' + str(notes_processed) + '.txt')
            ofd = open(note_outfile, 'a')
            ofd.write(note)
            ofd.close()
            notes_processed += 1

        note_idx += 1
