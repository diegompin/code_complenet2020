import os


def check_folder(folder):
    if not os.path.isdir(folder):
        os.makedirs(folder)


def get_filename(filename):
    return filename.replace(" ", "")
