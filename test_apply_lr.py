import unittest
from TransportationSplitter import *

class TestApplyLR(unittest.TestCase):
    apply_lr_params = [
        (
            "lr bigger than split", # case "label" to identify it more easily when fails
            1000,                   # original segment length (meters)
            [0, 0.8],               # original lr (relative)
            [0, 0.5],               # split start lr (relative)
            True,                   # expected is_applicable
            None,                   # expected new lr (relative)
        ),
        (
            "lr outside than split",
            1000, [0.7, 0.8], [0, 0.5], False, None),
        (
            "lr inside split",
            1000, [0.1, 0.4], [0, 0.5], True, [0.2, 0.8],
        ),
        (
            "lr inside split with snap",
            1000, [0, 0.499991], [0, 0.5], True, None,
        ),
        (
            "lr overlapping part small but keep",
            1000, [0.49998, 0.8], [0, 0.5], True, [0.99996, 1],
        ),                     
        (
            "lr overlapping part too small",
            1000, [0.499991, 0.8], [0, 0.5], False, None,
        ),             
    ]
    def test_apply_lr_on_split(self):
        for case_label, original_length_meters, original_lr, split_lr, expected_is_applicable, expected_new_lr in self.apply_lr_params:
            with self.subTest(case_label=case_label, 
                              original_length_meters=original_length_meters, 
                              original_lr=original_lr, 
                              split_lr=split_lr, 
                              expected_is_applicable=expected_is_applicable, 
                              expected_new_lr=expected_new_lr):
                split_segment = SplitSegment(None, None, 
                        SplitPoint(None, None, lr=split_lr[0], lr_meters=split_lr[0]*original_length_meters), 
                        SplitPoint(None, None, lr=split_lr[1], lr_meters=split_lr[1]*original_length_meters))
                is_applicable, new_lr = apply_lr_on_split(original_lr, split_segment, original_length_meters, min_overlapping_length_meters=0.01)

                self.assertEqual(is_applicable, expected_is_applicable, f"is_applicable mismatch for case '{case_label}'")
                self.assertEqual(new_lr, expected_new_lr, f"new_lr mismatch for case '{case_label}'")

unittest.main(argv=[''], verbosity=2, exit=False)
