<?php

// Endpoint: https://www.mcs.anl.gov/research/projects/waggle/downloads/datasets/metrics.php
// Description: Serves dataset size metrics.

printf("# HELP waggle_dataset_bytes Dataset size in bytes.\n");
printf("# TYPE waggle_dataset_bytes gauge\n");

foreach (glob("*.tar") as $filename) {
    printf("waggle_dataset_bytes{name=\"%s\"} %d\n", $filename, filesize($filename));
}

?>
