<?php

// Endpoint: https://web.lcrc.anl.gov/public/waggle/private/training_data/aot_audio_and_images/image_bottom/metrics.php
// Description: Serves training size metrics.
// 

printf("# HELP waggle_training_data_total Number of items in training data.\n");
printf("# TYPE waggle_training_data_total gauge\n");
// printf("# HELP waggle_training_data_bytes Total bytes of training data.\n");
// printf("# TYPE waggle_training_data_bytes gauge\n");

$resource = "image_bottom";

foreach (new FilesystemIterator(".") as $dir) {
    if (!$dir->isDir()) {
        continue;
    }

    $nodeID = $dir->getBasename();
    $total = 0;
    // $bytes = 0;

    foreach (new FilesystemIterator($dir) as $file) {
        $total += 1;
        // $bytes += $file->getSize();
    }

    printf("waggle_training_data_total{node_id=\"%s\",resource=\"%s\"} %d\n", $nodeID, $resource, $total);
    // printf("waggle_training_data_bytes{node_id=\"%s\",resource=\"%s\"} %d\n", $nodeID, $resource, $bytes);
}
?>
