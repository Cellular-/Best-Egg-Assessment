### The scenario:
- You have a vendor that delivers 2 files (`SOURCEDATA.TXT`, `SOURCECOLUMNS.txt`) every day at 6AM UTC. These files will land in `data/source/` for processing. For this assessment, you are provided the files in the `data/source/` path as if they were already delivered from the vendor.
- One file contains the raw pipe-delimited data (`SOURCEDATA.TXT`) without a header row.
- The other file (`SOURCECOLUMNS.txt`) contains pipe delimited data with 2 columns:
  - The first column contains an integer which represents *__the order in which these columns should
be displayed when combined with the data__* in `SOURCEDATA.TXT`. (__1__ being the first column)
  - The second column contains name of the column.


### The problem:
- The data must be loaded to a desired destination as a single file.
- This data needs to be represented in a single `.csv` file with a column row that is displaying the columns in the correct order as specified in column #1 of `SOURCECOLUMNS.txt`.

### Your requirements:
- Make your own copy of this repo on GitHub.
>I would recommend not forking the repository... unless you want the possibility of another candidate to get some great ideas from your solution, as a forked solution will be directly linked to this on GitHub. :)
- Write a program that shall use `main.py` as the entrypoint.
- The program shall take the data from both files and transform the data into a single __CSV__ file with both header row and data in a __comma-separated format__.
- The program shall sort the headers in `SOURCECOLUMNS.txt` so that they are written in the correct order when creating the final output `.csv` file.
- The final output `.csv` file shall not contain any data from column #1 of `SOURCECOLUMNS.txt`. Only the column name in the proper order is desired.
- The program shall load the newly generated __CSV__ file into `/data/destination` by using the `load_csv()` method in `SomeStorageLibrary` located in `src/some_storage_library.py`.
- Provide a link to your solution when you are complete.

> Note: I will simply be executing `python main.py` with the project's root directory as the current working directory. Whatever happens after that is up to you- just make sure the `.csv` you create exists in `data/destination/` after this runs (and that all of the requirements were met, of course).

> Note: use Python >= 3.7 to solve and do not use any other packages or dependencies (e.g. Pandas). You are limited to the Python Standard Library. The goal is to show us your coding style and see how you would approach the problem using plain Python.
