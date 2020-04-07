<?php

// Endpoint: https://web.lcrc.anl.gov/public/waggle/private/training_data/metrics.php
// Description: Serves training size metrics.
// 

printf("# HELP waggle_training_data_total Number of items in training data.\n");
printf("# TYPE waggle_training_data_total gauge\n");

function printResourceMetrics($basedir1, $basedir2, $subdir, $resource, $class) {
    $totals = [];

    // match directories like:
    // aot_audio_and_images/image_bottom/001e061182bb/2020-04-07T02:40:22+00:00.jpg
    // ----- basedir -------------------/---nodeID---/*.jpg
    try {
        foreach (new FilesystemIterator($basedir1) as $dir) {
            if (!$dir->isDir()) {
                continue;
            }

            $nodeID = $dir->getBasename();
            $total = (new GlobIterator($dir . "/*.jpg"))->count();

            if (!isset($totals[$nodeID])) {
                $totals[$nodeID] = 0;
            }

            $totals[$nodeID] += $total;
        }
    } catch (Exception $e) {
    }

    // match directories like:
    // aot_audio_and_images/good/001e061182bb/image/bottom/2020/04/07/2020-04-07T02:40:22+00:00.jpg
    // ----- basedir -----------/---nodeID---/---subdir---/YYYY/MM/DD/*.jpg
    try {
        foreach (new FilesystemIterator($basedir2) as $dir) {
            if (!$dir->isDir()) {
                continue;
            }

            $nodeID = $dir->getBasename();
            $total = (new GlobIterator($dir . "/" . $subdir . "/*/*/*/*.jpg"))->count();

            if (!isset($totals[$nodeID])) {
                $totals[$nodeID] = 0;
            }

            $totals[$nodeID] += $total;
        }
    } catch (Exception $e) {
    }

    foreach ($totals as $nodeID => $total) {
        printf("waggle_training_data_total{node_id=\"%s\",resource=\"%s\",class=\"%s\"} %d\n", $nodeID, $resource, $class, $total);
    }
}

printResourceMetrics("aot_audio_and_images/image_bottom", "aot_audio_and_images/good", "image/bottom", "image_bottom", "good");
printResourceMetrics("aot_audio_and_images/bad_image_bottom", "aot_audio_and_images/bad", "image/bottom", "image_bottom", "bad");
printResourceMetrics("aot_audio_and_images/error_image_bottom", "aot_audio_and_images/error", "image/bottom", "image_bottom", "error");

printResourceMetrics("aot_audio_and_images/image_top", "aot_audio_and_images/good", "image/top", "image_top", "good");
printResourceMetrics("aot_audio_and_images/bad_image_top", "aot_audio_and_images/bad", "image/top", "image_top", "bad");
printResourceMetrics("aot_audio_and_images/error_image_top", "aot_audio_and_images/error", "image/top", "image_top", "error");
?>
