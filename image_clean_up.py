#%%
import os
import datetime
 #%%
size = 0
max_size = 1200000000
min_size = 1000000000

path = r"C:/Users/Steve Cragg/Documents/disa3v2/images"
for path, dirs, files in os.walk(path):
    for f in files:
        fp = os.path.join(path, f)
        size += os.path.getsize(fp)
#%%
print(f'Folder size   = {size/1000/1000:.0f} MB\n\
Number of files = {len(files)}')

# %%
now = datetime.datetime.now()
# %%
int(now.timestamp())
# %%
if size >=1000000000:
    num_to_delete = int((size - min_size)/(size/len(files)))
    print(num_to_delete)
    for i in range(0,num_to_delete):
        os.remove(path +"/" +files[i])



