#!/usr/bin/env python
# encoding: utf-8
import configparser
import os

__author__ = "han"


def print_config_setting():
    print(os.path.abspath(__file__))
    print(os.path.dirname(os.path.abspath(__file__)))
    config_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_config.ini")
    print("配置文件信息：")
    config_parser = configparser.ConfigParser()
    file_name = config_parser.read(config_file_path)
    print(file_name)

    ini_file_sections = config_parser.sections()
    for section_name in ini_file_sections:
        options_in_section = config_parser.options(section_name)
        for option_name in options_in_section:
            print("[%s] %s = %s" % (section_name, option_name, config_parser.get(section_name, option_name)))


def read_config_section_option(section, option):
    config_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data_config.ini")
    config_parser = configparser.ConfigParser()
    config_parser.read(config_file_path)
    config_value = config_parser.get(section, option)
    return config_value


if __name__ == "__main__":
    print_config_setting()
