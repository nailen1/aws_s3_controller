# PyPI Upload Error: Invalid Distribution Metadata

## 문제 상황
PyPI에 패키지를 업로드하는 과정에서 다음과 같은 에러가 발생했습니다:
```
ERROR    InvalidDistribution: Invalid distribution metadata: unrecognized or malformed field 'license-file'
```

## 원인
이 문제는 setuptools와 twine 간의 메타데이터 처리 방식 차이로 인해 발생했습니다. setuptools가 생성한 메타데이터가 유효하지 않은 형식이었고, twine 6.1.0에서 이를 처리하는 과정에서 문제가 발생했습니다.

## 해결 방법
다음 두 가지 방법 중 하나로 해결할 수 있습니다:

1. packaging 패키지 업데이트 (적용한 방법)
```bash
python -m pip install -U packaging
```
이는 twine이 packaging 24.2 이상 버전과 함께 작동할 때 메타데이터 처리 문제를 해결할 수 있기 때문입니다.

2. setuptools 설정 수정 (대체 방법)
`pyproject.toml` 파일에 다음 설정을 추가하여 유효하지 않은 메타데이터 필드 생성을 방지할 수 있습니다:
```toml
[tool.setuptools]
license-files = []
```

## 참고 자료
- [GitHub Issue: pypa/twine#1216](https://github.com/pypa/twine/issues/1216)
