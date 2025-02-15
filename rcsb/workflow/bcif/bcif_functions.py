import subprocess
import os
from rcsb.utils.io.MarshalUtil import MarshalUtil
from mmcif.api.DictionaryApi import DictionaryApi

def bcifconvert(infile:str, outfile:str, python_molstar_java:str, da:DictionaryApi, molstar_cmd:str=None) -> bool:
   options = ["python", "molstar"] # java not implemented
   if python_molstar_java not in options:
      print("error - language %s not yet supported" % python_molstar_java)
      return False
   if python_molstar_java == "molstar":
      return molstar_convert(infile, outfile, molstar_cmd)
   elif python_molstar_java == "python":
      return py_convert(infile, outfile, da)

def molstar_convert(infile:str, outfile:str, molstar_cmd:str) -> bool:
   args = ["node", molstar_cmd, "-i", infile, "-ob", outfile]
   try:
      p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=None, env=None)
      stdout, stderr = p.communicate()
      print("stdout %s" % stdout.decode("utf-8"))
      print("stderr %s" % stderr.decode("utf-8"))
      if p.returncode != 0:
         raise ValueError("error %d" % p.returncode)
   except ValueError:
      print("Problems generating %s from %s" % (outfile, infile))
      return False
   return True

def py_convert(infile: str, outfile: str, da: DictionaryApi) -> bool:
   mu = MarshalUtil()
   data = mu.doImport(infile, fmt="mmcif")
   try:
      result = mu.doExport(outfile, data, fmt="bcif", dictionaryApi=da)
      if not result:
         raise Exception()
   except Exception as e:
      print("error during bcif conversion")
      return False
   return True 

