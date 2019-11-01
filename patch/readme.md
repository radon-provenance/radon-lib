# Modification

nano /usr/local/lib/python3.6/site-packages/dse/cqlengine/connection.py
on l.80 add:
    del self.cluster_options['load_balancing_policy']


# Create patch
diff -Naur connection.py connection_new.py > patch_dse.patch


# Apply patch
patch /usr/local/lib/python3.6/site-packages/dse/cqlengine/connection.py < patch_dse.patch 


# Apply Patch in radon-web venv:

patch ~/ve/radon-web/lib/python3.6/site-packages/dse/cqlengine/connection.py < patch_dse.patch 

