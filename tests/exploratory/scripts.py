import fire
import yaml

class MyCli(object):
    """This contains just some filler functions to generate a small example and test behaviour."""


    def double(self, input:str, output: str):
        """Read a number from a string and then double it."""
        with open(input, "r") as f:
            d = yaml.safe_load(f)

        value = d["x"]
        value = value * 2

        with open(output, "w") as f:
            res = {"x": value}
            yaml.dump(res, f)

if __name__ == '__main__':
  fire.Fire(MyCli)




