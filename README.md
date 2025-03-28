MSA 기반 물류 관리 시스템 "물류ON"

<img src="https://github.com/user-attachments/assets/5abb22ef-cf31-4bf9-942e-c216eb58b56b" alt="서비스 소개" width="100%"/>


<br/>
<br/>

# 1. About Project
- 프로젝트명: MSA 기반 국내 물류 관리 및 배송 시스템 개발
- 프로젝트 목표:
    - 기존 모놀리식 시스템의 한계를 극복하기 위해 MSA 구조 도입, 각 도메인을 독립적으로 설계
    - DDD 관점 적용하여 물류 시스템을 체계적으로 모델링 및 개발
    - 실제 운영 환경에서도 활용 가능한 구조를 갖춘 시스템 구현
- 진행 기간: 2025년 4월 11일 ~ 4월 25일 (총 15일간)
- 시연 스크립트: 

<img src= "https://github.com/user-attachments/assets/a1b5d38f-4cd1-4e38-931b-8955289520cc">

<br/>
<br/>

# 2. Team7 Chill Guys
| 김소진 | 박소해 | 오혜민 | 조규성 | 주소연 |
|:------:|:------:|:------:|:------:|:------:|
| <img src="https://avatars.githubusercontent.com/sojinnuna" alt="김소진" width="150"> | <img src="https://avatars.githubusercontent.com/S2gamzaS2" alt="박소해" width="150"> | <img src="https://avatars.githubusercontent.com/oh-min" alt="오혜민" width="150"> | <img src="https://avatars.githubusercontent.com/Imnotcoderdude" alt="조규성" width="150"> | <img src="https://avatars.githubusercontent.com/jcowwk" alt="주소연" width="150"> |
| Lead | BE | BE | Tech | BE |
| [GitHub](https://github.com/sojinnuna) | [GitHub](https://github.com/S2gamzaS2) | [GitHub](https://github.com/oh-min) | [GitHub](https://github.com/Imnotcoderdude) | [GitHub](https://github.com/jcowwk) |

<br/>
<br/>

# 3. Tasks & Responsibilities (작업 및 역할 분담)
[조규성]
- **Eureka Server**:
    - 마이크로 서비스 등록 및 서비스 간 위치 탐색을 위한 등록 서버 구축
      
- **API Gateway**:
    - Spring Cloud Gateway 기반 API 게이트웨이 구성
    - 경로 기반 라우팅 설정 및 인증 필터
    - Eureka 연동을 통한 라우팅 자동화 구현
      
- **User Service**
    - 사용자 정보 등록, 조회, 수정, 권한 변경 등 사용자 기능 구현
    - JPA 기반 도메인 모델 설계 및 구현 (User, UserRole 등)
    - FeignClient를 통한 외부 마이크로서비스 통신 구조 설계

- **Auth Service**
    - 로그인 및 회원가입 기능 구현
    - JWT 기반 인증 로직 구현 (토큰 발급 및 검증)
    - Refresh Token 관리 및 재발급 로직 처리
    - Gateway와 연동하여 인증 필터 통과 시 토큰 기반 유저 정보 추출 처리
 
[오혜민]
- **Hub Service**
    - 허브 등록, 조회, 수정, 삭제 기능 구현
    - 허브 등록/수정/삭제 시, 허브 루트 자동 생성/수정/삭제 기능 연동
    - FeignClient를 이용해 외부 서비스와의 통신 처리
    - 
- **HubRoute Service**
    - 허브 루트 등록, 조회, 수정, 삭제 기능 구현
    - 카카오 API를 활용하여 허브 간 이동 거리 및 시간 계산 기능 구현
    - Redis를 활용한 캐싱 처리로 데이터 조회 속도 향상
    - 허브/루트 정보 변경 시, 관련 캐시 데이터 자동 삭제 처리
    - FeignClient를 이용한 외부 서비스 통신

[주소연]
- **Product Service**
    - 상품 등록, 조회, 수정, 삭제, 검색 기능 구현
    - FeignClient를 이용해 다양한 외부 서비스와의 통신 처리

- **Slack Service**
    - Slack API 연동
    - FeignClient를 이용해 다양한 외부 서비스와의 통신 처리

- **AI Service**
    - Gemini API 연동
    - FeignClient를 이용해 슬랙 서비스와의 통신 처리
 
[김소진]    
- **Company Service**
    - 업체 등록, 조회, 수정, 삭제, 검색 기능 구현
    - FeignClient를 활용한 외부 마이크로서비스 통신 구조 설계

- **Order Service**
    - 주문 등록, 조회, 수정, 삭제, 검색 기능 구현
    - JWT 기반 사용자 인증 및 권한 검증 로직 구현으로 보안 강화
    - FeignClient를 활용한 외부 마이크로서비스(배송, 상품 등) 연동 구조 설계
    - 주문 상태에 따른 로직 분기 처리 및 주문 이력 관리 기능 구현

[박소해]
- **Delivery Service**
    - 배송 등록, 조회, 수정, 삭제 기능 구현
    - FeignClient를 활용한 외부 마이크로서비스 통신 구조 설계

- **DeliveryManager Service**
    - 배송 담당자 등록, 조회, 수정, 삭제 기능 구현
    - 담당자 선정 시 관련 배송 정보 자동 연동 처리
    - Kafka를 활용한 이벤트 기반 아키텍처 설계로 서비스 간 비동기 처리 구현
    - Slack 연동을 통해 배송담당자 알림 자동화 처리
<br/>
<br/>

# 4. ERD
erdcloud : [https://www.erdcloud.com/d/sT8WDsAXXqfbB9Gay](https://www.erdcloud.com/d/JCRKQRxseMZY28368)
<br/>

<img src= https://github.com/user-attachments/assets/1427bac2-f2e2-4495-9910-e5e30c6e55a6>
<br/>
<br/>

# 5. Infra
<img src= https://github.com/user-attachments/assets/b274175e-5e61-461e-b4d9-6f2d1f69c674>

<br/>
<br/>
