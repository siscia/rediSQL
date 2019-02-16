import toml
import os
import shutil

# we start by getting the version we are releaseing
cargo = toml.load("Cargo.toml")
version = cargo['package']['version']

scratch_space = "build_scratch/"
target_dir = "releases/{}".format(version)
# we create the necessary directory releases/$vresion/
try:
    os.makedirs(target_dir)
except:
    pass
try:
    os.makedirs(scratch_space)
except:
    pass

for target in ["armv7-unknown-linux-gnueabihf", "x86_64-unknown-linux-gnu"]:
    for feature in ["", "--features=pro", "--features=trial"]:
        cmd = "CARGO_TARGET_DIR={} cargo build --release --target {} {}".format(scratch_space, target, feature)
        print("Executing:", cmd)
        os.system(cmd)
        result_file = "{}/{}/release/libredis_sql.so".format(scratch_space, target)
        suffix = ""
        if feature == "--features=pro":
            suffix += "_PRO"
        if feature == "--features=trial":
            suffix += "_TRIAL"
        if target == "armv7-unknown-linux-gnueabihf":
            suffix += "_ARM_v7"
        if target == "x86_64-unknown-linux-gnu":
            suffix += "_x86_64"
        shutil.copyfile(result_file, "{}/redisql{}.so".format(target_dir, suffix))

os.removedirs(scratch_space)
