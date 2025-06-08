from unittest import TestCase

from rb_publish import generate_new_version


class TestVersion(TestCase):
    def test_basic(self):
        self.assertEqual(generate_new_version("0.0.0"), "0.0.1")

    def test_basic_with_alpha_false(self):
        self.assertEqual(generate_new_version("0.0.0", is_alpha=False), "0.0.1")

    def test_normal_to_alpha(self):
        self.assertEqual(generate_new_version("0.0.0", is_alpha=True, git_rev="xxx"), "0.0.1-rc.1")

    def test_alpha_to_alpha(self):
        self.assertEqual(
            generate_new_version("0.1.0-rc.1", is_alpha=True, git_rev="xxx"),
            "0.1.0-rc.2",
        )

    def test_alpha_to_normal(self):
        self.assertEqual(generate_new_version("1.0.1-rc.1", git_rev="xxx"), "1.0.1")
