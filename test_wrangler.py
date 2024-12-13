import unittest
from unittest.mock import MagicMock
from transportation_splitter import *


class TestSplitterDataWrangler(unittest.TestCase):
    def setUp(self):
        self.spark = MagicMock()
        self.input_path = "input/path"
        self.output_path_prefix = "output/path"
        self.custom_read_hook = MagicMock()
        self.custom_exists_hook = MagicMock()
        self.custom_write_hook = MagicMock()

        self.wrangler = SplitterDataWrangler(
            input_path=self.input_path,
            output_path_prefix=self.output_path_prefix,
            custom_read_hook=self.custom_read_hook,
            custom_exists_hook=self.custom_exists_hook,
            custom_write_hook=self.custom_write_hook,
        )

    def test_read_hook_called(self):
        for step in SplitterStep:
            with self.subTest(step=step):
                self.wrangler.read(self.spark, step)
                if step == SplitterStep.read_input:
                    self.custom_read_hook.assert_called_once_with(
                        self.spark, step, self.input_path
                    )
                else:
                    self.custom_read_hook.assert_called_once_with(
                        self.spark, step, self.output_path_prefix
                    )
                self.custom_read_hook.reset_mock()

    def test_exists_hook_called(self):
        for step in SplitterStep:
            with self.subTest(step=step):
                self.wrangler.check_exists(self.spark, step)
                self.custom_exists_hook.assert_called_once_with(
                    self.spark, step, self.output_path_prefix
                )
                self.custom_exists_hook.reset_mock()

    def test_write_hook_called(self):
        for step in SplitterStep:
            with self.subTest(step=step):
                df = MagicMock()
                self.wrangler.write(df, step)
                self.custom_write_hook.assert_called_once_with(
                    df, step, self.output_path_prefix
                )
                self.custom_write_hook.reset_mock()

    def test_paths(self):
        self.assertEqual(self.wrangler.input_path, "input/path")
        self.assertEqual(self.wrangler.output_path_prefix, "output/path")

    def test_bad_hook_constructor(self):
        with self.assertRaises(AssertionError):
            SplitterDataWrangler(
                "input/path",
                "output/path",
                custom_read_hook=lambda _spark, _step, _path: None,
            )

    def test_default_path_for_step(self):
        wrangler = SplitterDataWrangler("input/path", "output/path")
        self.assertEqual(
            wrangler.default_path_for_step(SplitterStep.read_input), "input/path"
        )
        self.assertEqual(
            wrangler.default_path_for_step(SplitterStep.spatial_filter),
            "output/path_1_spatially_filtered",
        )
        self.assertEqual(
            wrangler.default_path_for_step(SplitterStep.joined), "output/path_2_joined"
        )
        self.assertEqual(
            wrangler.default_path_for_step(SplitterStep.raw_split),
            "output/path_3_raw_split",
        )
        self.assertEqual(
            wrangler.default_path_for_step(SplitterStep.segment_splits_exploded),
            "output/path_4_segments_splits",
        )
        self.assertEqual(
            wrangler.default_path_for_step(SplitterStep.final_output),
            "output/path_final",
        )
