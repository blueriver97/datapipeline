#!/usr/bin/env bash

# ---------------------------------------------------------
# 색상 정의
# ---------------------------------------------------------
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 상태별 색상 적용 태그
SUCCESS_TAG="${GREEN}[성공]${NC}"
FAILURE_TAG="${RED}[실패]${NC}"
WARN_TAG="${YELLOW}[주의]${NC}"
INFO_TAG="[진행]"

# ---------------------------------------------------------
# 함수
# ---------------------------------------------------------
setup_git_config() {
  git config core.editor "vim"
  git config core.quotepath false
  git config fetch.prune true
  git config merge.ff true
  git config alias.recommit "commit --amend"
  return
}

check_error() {
    local exit_code=$1  # 첫 번째 인자: 실행 결과(Exit Code)
    local message=$2    # 두 번째 인자: 출력할 메시지

    if [[ $exit_code -ne 0 ]]; then
        echo -e "${FAILURE_TAG} $message"
        echo -e "${RED}오류가 발생하여 스크립트를 중단합니다.${NC}"
        exit 1
    else
        echo -e "${SUCCESS_TAG} $message"
    fi
    return
}

echo "프로젝트 초기 설정을 시작합니다..."
# ---------------------------------------------------------
# 1. GitHub CLI 및 Secrets 설정
# ---------------------------------------------------------
if ! command -v gh &> /dev/null; then
    echo -e "${WARN_TAG} GitHub CLI(gh) 미설치: SONAR_TOKEN 등록 건너뜀"
    echo "   [OS별 설치 가이드]"

    # OS 식별 및 가이드 출력
    if [[ "$OSTYPE" == "darwin"* ]]; then
        echo "   * macOS: brew install gh"
    elif [[ -f /etc/os-release ]]; then
        . /etc/os-release
        case "$ID" in
            ubuntu|debian)
                echo "   * Ubuntu/Debian:"
                echo "     1) (간편) sudo apt install gh"
                echo "     2) (공식) curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | sudo dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg"
                echo "        && sudo apt update && sudo apt install gh"
                ;;
            centos|rhel|amzn)
                echo "   * CentOS/RHEL/Amazon Linux:"
                echo "     1) sudo dnf config-manager --add-repo https://cli.github.com/packages/rpm/gh-cli.repo"
                echo "     2) sudo dnf install gh"
                ;;
            *)
                echo "   * Linux: https://github.com/cli/cli/blob/trunk/docs/install_linux.md"
                ;;
        esac
    else
        echo "   * 기타: https://cli.github.com/manual"
    fi
else
    # 로그인 체크
    if ! gh auth status &> /dev/null; then
        echo -e "${WARN_TAG} GitHub CLI 로그인"
        gh auth login
    else
        # 이미 등록된 시크릿 확인
        if gh secret list | grep -q "SONAR_TOKEN"; then
            echo -e "${SUCCESS_TAG} SONAR_TOKEN 이미 설정됨"
        else
            # 등록되어 있지 않을 때만 입력 받기
            if [[ -z "$SONAR_TOKEN" ]]; then
                read -sp "   > SonarCloud Token 입력 (없으면 Enter): " INPUT_TOKEN
                echo ""
                if [[ -n "$INPUT_TOKEN" ]]; then
                    export SONAR_TOKEN="$INPUT_TOKEN"
                fi
            fi

            # GitHub 저장소에 시크릿 업로드
            if [[ -n "$SONAR_TOKEN" ]]; then
                gh secret set SONAR_TOKEN --body "$SONAR_TOKEN"
                check_error $? "GitHub Repository Secrets에 SONAR_TOKEN 등록"
            else
                echo -e "${WARN_TAG} 토큰 미입력: 시크릿 등록 건너뜀"
            fi
        fi
    fi
fi

# ---------------------------------------------------------
# 2. 의존성 패키지 설치
# ---------------------------------------------------------
if [[ -f "package.json" ]]; then
    echo "   > NPM 패키지 설치 중..."
    npm install > /dev/null 2>&1
    check_error $? "NPM 패키지 설치 완료"
else
    echo -e "${WARN_TAG} package.json 없음: 건너뜀"
fi

# pre-commit 설치
if ! command -v pre-commit &> /dev/null; then
    echo "   > pre-commit 도구 설치 중..."
    pip install pre-commit > /dev/null 2>&1 || brew install pre-commit > /dev/null 2>&1

    if ! command -v pre-commit &> /dev/null; then
         check_error 1 "pre-commit 설치 실패 (수동 설치 필요: pip install pre-commit)"
    else
         echo -e "${SUCCESS_TAG} pre-commit 도구 설치 완료"
    fi
else
    echo -e "${SUCCESS_TAG} pre-commit 이미 설치됨"
fi

# TruffleHog 설치 (보안 검사)
if ! command -v trufflehog &> /dev/null; then
    echo "   > TruffleHog 설치 중..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew install trufflehog > /dev/null 2>&1
    else
        curl -sSfL https://raw.githubusercontent.com/trufflesecurity/trufflehog/main/scripts/install.sh | sudo sh -s -- -b /usr/local/bin > /dev/null 2>&1
    fi

    if ! command -v trufflehog &> /dev/null; then
         check_error 1 "TruffleHog 설치 실패 (수동 설치 필요)"
    else
         echo -e "${SUCCESS_TAG} TruffleHog 설치 완료"
    fi
else
    echo -e "${SUCCESS_TAG} TruffleHog 이미 설치됨"
fi

# Wily 설치 및 초기 데이터 생성
if ! command -v wily &> /dev/null; then
    echo "   > Wily(코드 복잡도 분석) 설치 중..."
    pip install wily > /dev/null 2>&1

    if ! command -v wily &> /dev/null; then
         check_error 1  "Wily 설치 실패 (수동 설치 필요)"
    else
         echo -e "${SUCCESS_TAG} Wily 도구 설치 완료"
    fi
else
    echo -e "${SUCCESS_TAG} Wily 이미 설치됨"
fi

# Wily Build (존재하는 경우에만 실행)
if command -v wily &> /dev/null; then
    echo "   > Wily 초기 데이터(Cache) 생성 중..."
    # Git 기록이 없거나 파일이 없으면 실패할 수 있으므로 에러 무시 처리
    wily build src > /dev/null 2>&1
    check_error $? "Wily 초기 설정 완료"
fi
# ---------------------------------------------------------
# 3. OpenCommit 설정
# ---------------------------------------------------------
if [[ -z "$GEMINI_API_KEY" ]]; then
    read -sp "   > Gemini API Key 입력 (없으면 Enter): " INPUT_KEY
    echo ""
    if [[ -n "$INPUT_KEY" ]]; then
        export GEMINI_API_KEY="$INPUT_KEY"
    fi
fi

if [[ -n "$GEMINI_API_KEY" ]]; then
    echo "   > OpenCommit 설정 적용 중..."
    npx oco config set OCO_API_KEY="$GEMINI_API_KEY" > /dev/null 2>&1
    npx oco config set OCO_AI_PROVIDER=gemini > /dev/null 2>&1
    npx oco config set OCO_MODEL=gemini-2.5-flash-lite > /dev/null 2>&1
    npx oco config set OCO_LANGUAGE=ko > /dev/null 2>&1
    npx oco config set OCO_GITPUSH=false > /dev/null 2>&1
    npx oco config set OCO_TOKENS_MAX_OUTPUT=512 > /dev/null 2>&1
    npx oco config set OCO_TOKENS_MAX_INPUT=40960 > /dev/null 2>&1
    npx oco config set OCO_ONE_LINE_COMMIT=true > /dev/null 2>&1
    check_error $? "OpenCommit 설정 완료"
else
    echo -e "${WARN_TAG} GEMINI_API_KEY 미입력: 설정 건너뜀"
fi

# ---------------------------------------------------------
# 4. Git Hook 설치 & 사용자 및 기본 설정
# ---------------------------------------------------------
pre-commit install --hook-type pre-commit --hook-type commit-msg --hook-type prepare-commit-msg > /dev/null 2>&1
check_error $? "Git Hooks 설치 완료"

if [[ -z "$GIT_USER" ]]; then
    read -p "   > Git Username 입력: " INPUT_USER

    if [[ -n "$INPUT_USER" ]]; then
        export GIT_USER="$INPUT_USER"
    fi

fi

if [[ -z "$GIT_EMAIL" ]]; then
    read -p "   > Git E-mail 입력: " INPUT_EMAIL

    if [[ -n "$INPUT_EMAIL" ]]; then
        export GIT_EMAIL="$INPUT_EMAIL"
    fi
fi
check_error $? "Git 사용자 설정 완료"

git config user.name "$GIT_USER"
git config user.email "$GIT_EMAIL"
setup_git_config
# ---------------------------------------------------------
# 5. SonarQube 프로젝트 키 설정
# ---------------------------------------------------------
SONAR_FILE="sonar-project.properties"
if [[ -f "$SONAR_FILE" ]]; then
    CURRENT_DIR_NAME=$(basename "$PWD")
    ORG_NAME=$(grep "^sonar.organization=" "$SONAR_FILE" | cut -d'=' -f2)

    if [[ -z "$ORG_NAME" ]]; then ORG_NAME="blueriver97"; fi
    NEW_KEY="${ORG_NAME}_${CURRENT_DIR_NAME}"

    if [[ "$OSTYPE" == "darwin"* ]]; then
        sed -i '' "s/^sonar.projectKey=.*/sonar.projectKey=$NEW_KEY/" "$SONAR_FILE"
    else
        sed -i "s/^sonar.projectKey=.*/sonar.projectKey=$NEW_KEY/" "$SONAR_FILE"
    fi
    check_error $? "SonarQube Project Key 변경: $NEW_KEY"
else
    echo -e "${WARN_TAG} SonarQube 설정 파일($SONAR_FILE) 없음"
fi

echo -e "${GREEN}모든 설정이 완료됨.${NC}"
