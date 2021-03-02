# TDT4305---Big-Data---Part-One
This repository is a part of an assignment in the course TDT4305 - Big Data Architectures, and consists of several tasks regarding the extraction and manipulation of data in large data-sets. 


The code is split into three distinct .py files, one for each task. To run the subtasks from tasks 1 and 2, the python file can be run, with the path to the directory containing the datasets as an argument. There is no need to specify the datasets themselves, only the directory containing the datasets. 

Example: python task2.py /user/example/data

For task 3, a CSV file is saved containing the results from the dataframe in subtask 2. In order to specify the directory path where the output should be saved, this needs to be entered as an argument after the directory path to the datasets. A “output.csv” file will be created in a “output” folder, in the specified directory. To run the subtasks in task 3: python task3.py directory-path output-path


Example: python task3.py /local/user/data   /local/user/data/results

Figure 1 shows the dataset-structure, and figure 2 shows the subtasks performed in each script. 

<p align="center">
<img src="https://github.com/thomasfosen/TDT4305---Big-Data---Part-One/blob/main/figures/dataDescription.png" width="600"><br>
Figure 1: Data Description
</p>


<p align="center">
<img src="https://github.com/thomasfosen/TDT4305---Big-Data---Part-One/blob/main/figures/problemDescription.png" width="600"><br>
Figure 2: Problem Description
</p>
