<?php

// Endpoint: https://web.lcrc.anl.gov/public/waggle/private/training_data/metrics.php
// Description: Serves training size metrics.
// 

printf("# HELP waggle_training_data_total Number of items in training data.\n");
printf("# TYPE waggle_training_data_total gauge\n");

// Having to match patterns like:
// aot_audio_and_images/image_bottom/001e061182bb/2020-04-07T02:40:22+00:00.jpg
// ----- basedir -------------------/---nodeID---/*.jpg
function printResourceMetrics1($basedir, $resource, $class) {
    foreach (new FilesystemIterator($basedir) as $dir) {
        if (!$dir->isDir()) {
            continue;
        }

        $nodeID = $dir->getBasename();
        $total = (new GlobIterator($dir . "/*.jpg"))->count();

        printf("waggle_training_data_total{node_id=\"%s\",resource=\"%s\",class=\"%s\"} %d\n", $nodeID, $resource, $class, $total);
    }
}

// Having to match patterns like:
// aot_audio_and_images/good/001e061182bb/image/bottom/2020/04/07/2020-04-07T02:40:22+00:00.jpg
// ----- basedir -----------/---nodeID---/---subdir---/YYYY/MM/DD/*.jpg
function printResourceMetrics2($basedir, $subdir, $resource, $class) {
    foreach (new FilesystemIterator($basedir) as $dir) {
        if (!$dir->isDir()) {
            continue;
        }

        $nodeID = $dir->getBasename();
        $total = (new GlobIterator($dir . "/" . $subdir . "/*/*/*/*.jpg"))->count();

        printf("waggle_training_data_total{node_id=\"%s\",resource=\"%s\",class=\"%s\"} %d\n", $nodeID, $resource, $class, $total);
    }
}

printResourceMetrics1("aot_audio_and_images/image_bottom", "image_bottom", "good");
printResourceMetrics1("aot_audio_and_images/bad_image_bottom", "image_bottom", "bad");
printResourceMetrics1("aot_audio_and_images/image_top", "image_top", "good");

printResourceMetrics2("aot_audio_and_images/good", "image/bottom", "image_bottom", "good");
printResourceMetrics2("aot_audio_and_images/bad", "image/bottom", "image_bottom", "bad");
printResourceMetrics2("aot_audio_and_images/error", "image/bottom", "image_bottom", "error");
?>
