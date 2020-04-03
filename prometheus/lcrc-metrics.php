<?php

// Endpoint: https://web.lcrc.anl.gov/public/waggle/private/training_data/metrics.php
// Description: Serves training size metrics.
// 

printf("# HELP waggle_training_data_total Number of items in training data.\n");
printf("# TYPE waggle_training_data_total gauge\n");

function printResourceMetrics($basedir, $resource, $class) {
    foreach (new FilesystemIterator($basedir) as $dir) {
        if (!$dir->isDir()) {
            continue;
        }

        $nodeID = $dir->getBasename();
        $total = (new GlobIterator($dir . "/*.jpg"))->count();

        printf("waggle_training_data_total{node_id=\"%s\",resource=\"%s\",class=\"%s\"} %d\n", $nodeID, $resource, $class, $total);
    }
}

printResourceMetrics("aot_audio_and_images/image_bottom", "image_bottom", "good");
printResourceMetrics("aot_audio_and_images/image_top", "image_top", "good");
?>
