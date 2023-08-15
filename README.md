# CondorCMF
### A framework for implementing  coordinator-follower and coordinator-manager-follower networks on vanilla HTCondor

<!-- tempate https://github.com/scottydocs/README-template.md/blob/master/README.md -->
![GitHub repo size](https://img.shields.io/github/repo-size/mjcarter95/CondorCMF)
![GitHub contributors](https://img.shields.io/github/contributors/mjcarter95/CondorCMF)
![GitHub stars](https://img.shields.io/github/stars/mjcarter95/CondorCMF?style=social)
![GitHub forks](https://img.shields.io/github/forks/mjcarter95/CondorCMF?style=social)
![Twitter Follow](https://img.shields.io/twitter/follow/mjcarter955?style=social)

CondorCMF is a Python package that allows users to schedule tasks following a coordinator-follower and coordinator-manager-follower paradigm on HTCondor. CondorCMF utilises a database to allow daemons in the vanilla HTCondor universe to communicate with one another.

## Installing CondorCMF
To install CondorCMF, follow these steps:

```
pip install pip@git+https://github.com/mjcarter95/CondorCMF.git
```

## Using CondorCMF
A number of example problems are provided in the `examples` folder.

## Contributing to CondorCMF
To contribute to CondorCMF, follow these steps:

1. Fork this repository.
2. Create a branch: `git checkout -b <branch_name>`.
3. Make your changes and commit them: `git commit -m '<commit_message>'`
4. Push to the original branch: `git push origin <project_name>/<location>`
5. Create the pull request.

Alternatively see the GitHub documentation on [creating a pull request](https://help.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request).

## Authors
* Matthew Carter - [mjcarter.co](https://mjcarter.co)
* Ian Smith
* Edward Pyzer-Knapp
* Paul Spirakis
* Simon Maskell - [simonmaskell.com](https://simonmaskell.com)

## Contact
If you want to contact me you can reach me at <m (dot) j (dot) carter (at) liverpool (dot) ac (dot) uk>.

## Citation
We appreciate citations as they let us discover what people have been doing with the software. 

To cite CondorSMC in publications use:

Carter, M., Smith, I., Pyzer-Knapp, K., Spirakis, P. Maskell, S.,(2023). CondorSMC (1.0.0). https://github.com/mjcarter95/CondorSMC

Or use the following BibTeX entry:

```
@misc{CondorSMC,
  title = {CondorSMC (1.0.0)},
  author = {Carter, Matthew and Smith, Ian and Pyzer-Knapp, Edward and Spirakis, Paul and Maskell, Simon},
  year = {2023},
  month = may,
  howpublished = {GitHub},
  url = {https://github.com/mjcarter95/CondorSMC}
}
```
