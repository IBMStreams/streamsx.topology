Name SPL files or SPLMM files that do not live in a namespace directory

- test_no_ns.spl.tmpl     (pure SPL files)
- test_no_ns.splmm.tmpl   (SPLMM files)

Artifacts in the namespace directory tmp.samplemain must be named

- test.spl.tmpl     (pure SPL files)
- test.splmm.tmpl   (SPLMM files)

Otherwise the scripts create_test_sc_files.sh and delete_test_sc_files.sh won't work properly.